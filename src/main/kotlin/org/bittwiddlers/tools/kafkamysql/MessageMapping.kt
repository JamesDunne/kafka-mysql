package org.bittwiddlers.tools.kafkamysql

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.streams.kstream.Predicate
import org.apache.kafka.streams.kstream.ValueMapperWithKey
import java.io.InputStreamReader
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.sql.Types
import java.time.Instant
import java.util.function.Function
import javax.script.ScriptEngine

class MappingRoot {
  var groovy: String? = null
  var topics: LinkedHashMap<String, Topic>? = null

  fun configure(parent: Path, scriptEngine: ScriptEngine) {
    val filename = groovy ?: return
    InputStreamReader(Files.newInputStream(parent.resolve(filename))).use {
      scriptEngine.eval(it)
    }
  }
}

class Topic {
  var mappings: List<MessageMapping>? = null
}

class MessageMapping {
  var preprocess: List<PreprocessStep>? = null
  var tables: LinkedHashMap<String, Table>? = null

  @Suppress("UNCHECKED_CAST")
  fun configure(topicName: String, mappingIndex: Int, scriptEngine: ScriptEngine) {
    for ((i, step) in (preprocess ?: emptyList()).withIndex()) {
      // filter:
      step.filterPredicate = step.filter?.let { code ->
        val clazz = scriptEngine.eval(
          """
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.*
class Filter_${topicName}_${mappingIndex}_${i} implements Predicate<String, Map<String, Object>> {
  @Override
  boolean test(String k, Map<String, Object> v) {
    /**/ $code /**/
  }
}"""
        )
        clazz as Class<Predicate<String, JsonObjectGeneral>>
      }?.newInstance()

      // map:
      step.mapFunction = step.map?.let { code ->
        val clazz = scriptEngine.eval(
          """
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.*
class Map_${topicName}_${mappingIndex}_${i} implements ValueMapperWithKey<String, Map<String, Object>, Map<String, Object>> {
  @Override
  Map<String, Object> apply(String k, Map<String, Object> v) {
    /**/ $code /**/
  }
}"""
        )
        clazz as Class<ValueMapperWithKey<String, JsonObjectGeneral, JsonObjectGeneral>>
      }?.newInstance()
    }

    for (table in tables ?: emptyMap()) {
      table.value.configure(table.key, scriptEngine)
    }
  }
}

class PreprocessStep {
  // these are all mutually exclusive; only one function should be specified for each preprocess step:
  var filter: String? = null
  var map: String? = null

  // passed kafka message key and value:
  var filterPredicate: Predicate<String, JsonObjectGeneral>? = null
  var mapFunction: ValueMapperWithKey<String, JsonObjectGeneral, JsonObjectGeneral>? = null
}

class Table {
  var mapped: MappedTable? = null
  var flatMapped: FlatMappedTable? = null

  @JsonIgnore
  lateinit var tableName: String

  fun configure(tableName: String, scriptEngine: ScriptEngine) {
    this.tableName = tableName
    flatMapped?.configure(tableName, scriptEngine)
    mapped?.configure(tableName, scriptEngine)
  }
}

class MappedTable {
  var columns: LinkedHashMap<String, Column>? = null

  @JsonIgnore
  lateinit var tableName: String

  fun configure(tableName: String, scriptEngine: ScriptEngine) {
    this.tableName = tableName

    // set up property selectors for columns:
    for (col in columns ?: emptyMap()) {
      col.value.configure(tableName, col.key, scriptEngine)
    }
  }
}

class FlatMappedTable {
  var from: String? = null

  @JsonIgnore
  var fromSelector: Function<Map<String, Any?>, List<JsonObjectGeneral?>>? = null

  var columns: LinkedHashMap<String, Column>? = null

  var delete: FlatMapDelete? = null

  @JsonIgnore
  lateinit var tableName: String

  @Suppress("UNCHECKED_CAST")
  fun configure(tableName: String, scriptEngine: ScriptEngine) {
    this.tableName = tableName

    // set up list selector for flatMap:
    fromSelector = from?.let {
      scriptEngine.eval(
        """
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.*
class Select${tableName}FlatMapped implements java.util.function.Function<Map<String, Object>, List<Map<String, Object>>> {
  @Override public List<Map<String, Object>> apply(Map<String, Object> v) {
    return /**/ $it /**/;
  }
}"""
      ) as Class<Function<Map<String, Any?>, List<JsonObjectGeneral?>>>
    }?.newInstance()

    // set up property selectors for columns:
    for (col in columns ?: emptyMap()) {
      col.value.configure(tableName, col.key, scriptEngine)
    }
  }
}

class FlatMapDelete {
  var where: List<String>? = null
  var whereNotIn: String? = null
}

class Column {
  // mysql data type
  var type: String? = null

  // json path relative to scoped object
  var from: String? = null
  var fromRoot: String?
    get() = from
    set(value) {
      from = value
      isRelativeToRoot = true
    }

  @JsonIgnore
  var isRelativeToRoot: Boolean = false

  // is part of primary key?
  var primaryKey: Boolean = false

  @JsonIgnore
  var fromSelector: Function<JsonObjectGeneral, Any?>? = null

  @JsonIgnore
  lateinit var tableName: String

  @JsonIgnore
  lateinit var columnName: String

  @Suppress("UNCHECKED_CAST")
  fun configure(tableName: String, columnName: String, scriptEngine: ScriptEngine) {
    this.tableName = tableName
    this.columnName = columnName

    fromSelector = from?.let {
      scriptEngine.eval(
        """
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.*
class Select${columnName}From${tableName} implements java.util.function.Function<Map<String, Object>, Object> {
  @Override public Object apply(Map<String, Object> v) {
    return /**/ $it /**/;
  }
}"""
      ) as Class<Function<JsonObjectGeneral, Any?>>
    }?.newInstance()
  }

  fun nameEqualsParamExpression(name: String = columnName) =
    "`$name`=${paramExpression()}"

  fun paramExpression() =
    when (type) {
      "uuid" -> "UUID_TO_BIN(?)"
      else -> "?"
    }

  fun readValue(jsonRoot: JsonObjectGeneral?, jsonObject: JsonObjectGeneral?): Any? =
    (if (isRelativeToRoot) jsonRoot else jsonObject)?.let {
      fromSelector!!.apply(it)
    }

  fun paramSetterForColumn(
    stmt: PreparedStatement,
    jsonRoot: JsonObjectGeneral?,
    jsonObject: JsonObjectGeneral?,
    json: ObjectMapper
  ): (Int) -> Unit {
    val v: Any? = readValue(jsonRoot, jsonObject)

    val type = this.type!!
    when {
      // handle NULL first:
      v == null -> return { n: Int -> stmt.setNull(n, sqlType()) }
      // serialize JSON as string:
      type == "json" -> {
        val jsonStr = json.writeValueAsString(v)
        return { n: Int -> stmt.setString(n, jsonStr) }
      }
      else -> when (v) {
        is String -> when {
          "datetime".equals(type, true) -> {
            val inst = Instant.parse(v)
            val tstp = Timestamp.from(inst)
            return { n: Int -> stmt.setTimestamp(n, tstp) }
          }
          "date".equals(type, true) -> {
            // TODO: test
            val inst = Instant.parse(v)
            val tstp = Timestamp.from(inst)
            return { n: Int -> stmt.setTimestamp(n, tstp) }
          }
          "uuid".equals(type, true) -> return { n: Int -> stmt.setString(n, v) }
          type.startsWith("varchar", true) -> return { n: Int -> stmt.setString(n, v) }
          type.startsWith("char", true) -> return { n: Int -> stmt.setString(n, v) }
          type.startsWith("nvarchar", true) -> return { n: Int -> stmt.setNString(n, v) }
          type.startsWith("nchar", true) -> return { n: Int -> stmt.setNString(n, v) }
          else -> throw RuntimeException("unhandled data type ${v.javaClass.name} : $type")
        }
        is Boolean -> return { n: Int -> stmt.setBoolean(n, v) }
        is Int -> return { n: Int -> stmt.setInt(n, v) }
        is Long -> return { n: Int -> stmt.setLong(n, v) }
        is BigDecimal -> return { n: Int -> stmt.setBigDecimal(n, v) }
        is Double -> return { n: Int -> stmt.setDouble(n, v) }
        is Float -> return { n: Int -> stmt.setFloat(n, v) }
        is ByteArray -> return { n: Int -> stmt.setBytes(n, v) }
        else -> throw RuntimeException("unhandled data type ${v.javaClass.name} : $type")
      }
    }
  }

  fun sqlType(): Int {
    val type = this.type!!
    return when {
      "uuid".equals(type, true) -> Types.BINARY
      type.startsWith("varchar", true) -> Types.VARCHAR
      "int".equals(type, true) -> Types.INTEGER
      "datetime".equals(type, true) -> Types.TIMESTAMP
      "bit".equals(type, true) -> Types.BIT
      "json".equals(type, true) -> Types.LONGVARCHAR // ???
      type.startsWith("char", true) -> Types.CHAR
      type.startsWith("decimal", true) -> Types.DECIMAL
      type.startsWith("varbinary", true) -> Types.VARBINARY
      type.startsWith("binary", true) -> Types.BINARY
      type.startsWith("nvarchar", true) -> Types.NVARCHAR
      type.startsWith("nchar", true) -> Types.NCHAR
      "date".equals(type, true) -> Types.DATE
      "double".equals(type, true) -> Types.DOUBLE
      "float".equals(type, true) -> Types.FLOAT
      "bigint".equals(type, true) -> Types.BIGINT
      // TODO: more type mappings
      else -> Types.JAVA_OBJECT
    }
  }
}
