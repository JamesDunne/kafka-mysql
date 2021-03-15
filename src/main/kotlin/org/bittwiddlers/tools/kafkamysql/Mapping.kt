package org.bittwiddlers.tools.kafkamysql

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.ObjectMapper
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import java.math.BigDecimal
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.sql.Types
import java.time.Instant

class MappingRoot {
  var topics: LinkedHashMap<String, Topic>? = null
}

class Topic {
  var events: List<Event>? = null
}

class Event {
  var filter: String? = null
  var tables: LinkedHashMap<String, Table>? = null
}

class Table {
  var entities: LinkedHashMap<String, Entity>? = null
  var flatMapped: Entity? = null
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

  // is part of primary key?
  var primaryKey: Boolean = false

  // is the flatMapped entityId? used for `DELETE WHERE primaryKeys=... AND entityId NOT IN (?,?,?,...)`
  var entityId: Boolean = false

  @JsonIgnore
  var isRelativeToRoot: Boolean = false

  // compiled json path:
  @JsonIgnore
  var fromPath: JsonPath? = null

  fun nameEqualsParamExpression(name: String) =
    "$name=${paramExpression()}"

  fun paramExpression() =
    when (type) {
      "uuid" -> "UUID_TO_BIN(?)"
      else -> "?"
    }

  fun readValue(jsonRoot: Any?, jsonObject: Any?, config: Configuration): Any? =
    when (jsonObject) {
      null -> null
      else -> {
        val values = fromPath!!.read<List<Any?>>(
          if (isRelativeToRoot) jsonRoot else jsonObject,
          config
        )
        when {
          values.size > 1 ->
            throw RuntimeException("column json-path cannot select more than one JSON property!")
          values.isEmpty() ->
            null
          else ->
            values[0]
        }
      }
    }

  fun paramSetterForColumn(
    stmt: PreparedStatement,
    jsonRoot: Any?,
    jsonObject: Any?,
    config: Configuration,
    json: ObjectMapper
  ): (Int) -> Unit {
    val v: Any? = readValue(jsonRoot, jsonObject, config)

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
          else -> throw RuntimeException("unhandled data type ${v?.javaClass?.name} : $type")
        }
        is Boolean -> return { n: Int -> stmt.setBoolean(n, v) }
        is Int -> return { n: Int -> stmt.setInt(n, v) }
        is Long -> return { n: Int -> stmt.setLong(n, v) }
        is BigDecimal -> return { n: Int -> stmt.setBigDecimal(n, v) }
        is Double -> return { n: Int -> stmt.setDouble(n, v) }
        is Float -> return { n: Int -> stmt.setFloat(n, v) }
        is ByteArray -> return { n: Int -> stmt.setBytes(n, v) }
        else -> throw RuntimeException("unhandled data type ${v?.javaClass?.name} : $type")
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

class Entity {
  // json path to root of entity containing non-dated properties
  // within itself and optionally a `dated` property containing
  // dated `entries` with more properties
  var from: String? = null

  // compiled json path:
  @JsonIgnore
  var fromPath: JsonPath? = null

  // properties to pull out of the non-dated entity root:
  var nondated: LinkedHashMap<String, Column>? = null

  // properties to pull out of the current dated entry:
  var dated: LinkedHashMap<String, Column>? = null
}
