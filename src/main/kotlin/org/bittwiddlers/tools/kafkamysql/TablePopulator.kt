package org.bittwiddlers.tools.kafkamysql

import com.fasterxml.jackson.databind.ObjectMapper
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option
import org.apache.kafka.streams.processor.Processor
import org.apache.kafka.streams.processor.ProcessorContext
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.SQLSyntaxErrorException
import java.time.Instant
import javax.sql.DataSource

class TablePopulator(
  val ds: DataSource,
  val json: ObjectMapper,
  val table: Map.Entry<String, Table>
) : Processor<String, JsonObject> {
  private var now: Instant = Instant.now()
  var colCount: Int = 0

  lateinit var stmtInsert: PreparedStatement
  var primaryKeyColumns: List<Map.Entry<String, Column>>? = null
  private lateinit var entityIdCol: Map.Entry<String, Column>

  val config: Configuration = Configuration.defaultConfiguration()
    .setOptions(
      Option.ALWAYS_RETURN_LIST,
      Option.SUPPRESS_EXCEPTIONS
    )

  lateinit var conn: Connection

  fun createTable() {
    val sql = StringBuilder()
    sql.append("create table if not exists ${table.key} (\n")

    val cols = mutableListOf<String>()
    val pkNames = mutableListOf<String>()
    val entities = table.value.entities
    if (entities != null) {
      for (entity in entities) {
        for (col in entity.value.nondated ?: emptyMap()) {
          if (col.value.primaryKey) {
            pkNames.add("`${col.key}`")
          }
          cols.add("`${col.key}` ${typeToMysql(col.value.type!!)}")
        }
        for (col in entity.value.dated ?: emptyMap()) {
          if (col.value.primaryKey) {
            pkNames.add("`${col.key}`")
          }
          cols.add("`${col.key}` ${typeToMysql(col.value.type!!)}")
        }
      }
    }
    val flatMapped = table.value.flatMapped
    if (flatMapped != null) {
      for (col in flatMapped.nondated ?: emptyMap()) {
        if (col.value.primaryKey) {
          pkNames.add("`${col.key}`")
        }
        cols.add("`${col.key}` ${typeToMysql(col.value.type!!)}")
      }
      for (col in flatMapped.dated ?: emptyMap()) {
        if (col.value.primaryKey) {
          pkNames.add("`${col.key}`")
        }
        cols.add("`${col.key}` ${typeToMysql(col.value.type!!)}")
      }
    }

    sql.append(cols.joinToString(",\n  "))
    if (pkNames.isNotEmpty()) {
      sql.append(",\n  primary key (${pkNames.joinToString(",")})\n")
    }
    sql.append(")")

    val sqlText = sql.toString()
    try {
      conn.createStatement().execute(sqlText)
    } catch (ex: SQLSyntaxErrorException) {
      throw RuntimeException("SQL syntax error in query:\n$sqlText", ex)
    }
  }

  override fun init(context: ProcessorContext?) {
    now = Instant.now()
    val colsInsName = mutableListOf<String>()
    val colsInsValue = mutableListOf<String>()
    val colsUpdExpr = mutableListOf<String>()
    val pkCols = mutableListOf<Map.Entry<String, Column>>()

    colCount = 0
    val flatMapped = table.value.flatMapped
    val entities = table.value.entities
    if (flatMapped != null) {
      flatMapped.fromPath = JsonPath.compile(flatMapped.from)

      for (col in flatMapped.nondated ?: emptyMap()) {
        col.value.fromPath = JsonPath.compile(col.value.from)

        colsInsName.add("`${col.key}`")
        val param = col.value.paramExpression()
        colsInsValue.add(param)
        val checkParam = "`${col.key}`=${param}"
        colsUpdExpr.add(checkParam)
        // track primaryKey columns for delete statement:
        if (col.value.entityId) {
          entityIdCol = col
        } else if (col.value.primaryKey) {
          pkCols.add(col)
        }

        colCount++
      }
      for (col in flatMapped.dated ?: emptyMap()) {
        col.value.fromPath = JsonPath.compile(col.value.from)

        colsInsName.add("`${col.key}`")
        val param = col.value.paramExpression()
        colsInsValue.add(param)
        val checkParam = "`${col.key}`=${param}"
        colsUpdExpr.add(checkParam)
        // primaryKeys cannot be dated properties:
        if (col.value.primaryKey) {
          throw RuntimeException("primaryKey cannot be set on dated properties")
        }

        colCount++
      }
    } else if (entities != null) {
      for (entity in entities) {
        entity.value.fromPath = JsonPath.compile(entity.value.from)

        for (col in entity.value.nondated ?: emptyMap()) {
          col.value.fromPath = JsonPath.compile(col.value.from)

          colsInsName.add("`${col.key}`")
          val param = col.value.paramExpression()
          colsInsValue.add(param)
          val checkParam = "`${col.key}`=${param}"
          colsUpdExpr.add(checkParam)

          colCount++
        }
        for (col in entity.value.dated ?: emptyMap()) {
          col.value.fromPath = JsonPath.compile(col.value.from)

          colsInsName.add("`${col.key}`")
          val param = col.value.paramExpression()
          colsInsValue.add(param)
          val checkParam = "`${col.key}`=${param}"
          colsUpdExpr.add(checkParam)
          if (col.value.primaryKey) {
            throw RuntimeException("primaryKey cannot be set on dated properties")
          }

          colCount++
        }
      }
    }

    primaryKeyColumns = pkCols

    conn = ds.connection
    conn.autoCommit = false

    val sql = StringBuilder()
    sql.append("insert into `${table.key}` (")
    sql.append(colsInsName.joinToString(","))
    sql.append(") values (")
    sql.append(colsInsValue.joinToString(","))
    sql.append(") on duplicate key update ")
    sql.append(colsUpdExpr.joinToString(","))
    stmtInsert = conn.prepareStatement(sql.toString())

    createTable()
  }

  override fun process(k: String?, jsonRoot: JsonObject?) {
    val flatMapped = table.value.flatMapped
    val entities = table.value.entities
    if (flatMapped != null) {
      processFlatMapped(flatMapped, jsonRoot)
    } else if (entities != null) {
      processEntities(entities, jsonRoot)
    }
  }

  private fun processEntities(
    entities: LinkedHashMap<String, Entity>,
    jsonRoot: JsonObject?
  ) {
    try {
      var n = 1
      stmtInsert.clearParameters()
      for (entity in entities) {
        val t = entity.value.fromPath!!.read<List<Any?>>(jsonRoot, config)
        val entityObj = when {
          t.size > 1 -> throw RuntimeException("table ${table.key} entity ${entity.key} cannot select multiple JSON objects!")
          t.isEmpty() -> null
          else -> t[0]
        }

        n = addColValuesAsParams(
          stmtInsert,
          n,
          colCount,
          entity.value.nondated ?: emptyMap(),
          jsonRoot,
          entityObj,
          config
        )

        val datedMap = getDatedObject(entityObj)
        val entryObj = when {
          datedMap != null -> when {
            // null out this entity if it's deleted:
            !isDatedAlive(datedMap, now) -> null
            // find the applicable dated entry first:
            else -> findDatedEntry(datedMap, now)
          }
          // fill with NULL values:
          else -> null
        }

        n = addColValuesAsParams(
          stmtInsert,
          n,
          colCount,
          entity.value.dated ?: emptyMap(),
          jsonRoot,
          entryObj,
          config
        )
      }

      stmtInsert.executeUpdate()
      conn.commit()
    } catch (ex: SQLException) {
      conn.rollback()
      throw ex
    }
  }

  private fun processFlatMapped(flatMapped: Entity, jsonRoot: JsonObject?) {
    val entityObjects = flatMapped.fromPath!!.read<List<Any?>>(jsonRoot)

    // find how many alive entities there are:
    val aliveEntityObjs = entityObjects.filter { entityObj ->
      val datedMap = getDatedObject(entityObj) ?: return@filter true
      return@filter isDatedAlive(datedMap, now)
    }

    // start a batch:
    stmtInsert.clearBatch()

    loop@ for (entityObj in aliveEntityObjs) {
      stmtInsert.clearParameters()

      val datedMap = getDatedObject(entityObj)
      val entryObj = when {
        // find the applicable dated entry first:
        datedMap != null -> findDatedEntry(datedMap, now)
        // fill with NULL values:
        else -> null
      }

      var n = 1
      n = addColValuesAsParams(
        stmtInsert,
        n,
        colCount,
        flatMapped.nondated ?: emptyMap(),
        jsonRoot,
        entityObj,
        config
      )

      n = addColValuesAsParams(
        stmtInsert,
        n,
        colCount,
        flatMapped.dated ?: emptyMap(),
        jsonRoot,
        entryObj,
        config
      )

      stmtInsert.addBatch()
    }

    try {
      // execute the UPSERT batch:
      stmtInsert.executeBatch()

      // delete entities not in the alive list:
      val delSql = StringBuilder()
      delSql.append(
        "delete from `${table.key}` where ${
          primaryKeyColumns?.joinToString(
            " and ",
            transform = { it.value.nameEqualsParamExpression(it.key) })
        }"
      )
      if (aliveEntityObjs.isNotEmpty()) {
        delSql.append(" and `${entityIdCol.key}` not in (${
          aliveEntityObjs.joinToString { entityIdCol.value.paramExpression() }
        })")
      }

      val sqlText = delSql.toString()
      conn.prepareStatement(sqlText).use {
        var n = 1
        for (col in primaryKeyColumns ?: emptyList()) {
          col.value.paramSetterForColumn(
            it,
            jsonRoot,
            jsonRoot,
            config,
            json
          )(n)
          n++
        }
        for (entityObj in aliveEntityObjs) {
          entityIdCol.value.paramSetterForColumn(
            it,
            jsonRoot,
            entityObj,
            config,
            json
          )(n)
          n++
        }

        try {
          it.executeUpdate()
        } catch (ex: SQLSyntaxErrorException) {
          throw RuntimeException("SQL syntax error in query:\n$sqlText", ex)
        }
      }

      conn.commit()
    } catch (ex: SQLException) {
      conn.rollback()
      throw ex
    }
  }

  override fun close() {
    conn.close()
  }

  private fun getDatedObject(entityObj: Any?): LinkedHashMap<String, Any?>? {
    if (entityObj == null) return null

    val entityMap = (entityObj as LinkedHashMap<String, Any?>?) ?: return null

    val datedObj = entityMap["dated"]
    val datedMap = (datedObj as LinkedHashMap<String, Any?>?) ?: return null

    return datedMap
  }

  private fun isDatedAlive(
    datedMap: LinkedHashMap<String, Any?>,
    now: Instant
  ): Boolean {
    val createdOnStr = datedMap["createdOn"] as String?
    val createdOn = createdOnStr?.let { Instant.parse(it) } ?: return false
    if (now.isBefore(createdOn)) return false

    val deletedOnStr = datedMap["deletedOn"] as String?
    val deletedOn = deletedOnStr?.let { Instant.parse(it) } ?: return true
    if (now.isAfter(deletedOn)) return false

    return true
  }

  private fun findDatedEntry(
    datedMap: LinkedHashMap<String, Any?>,
    now: Instant
  ): Any? {
    val entries = datedMap["entries"]
    val entriesList = (entries as List<LinkedHashMap<String, Any?>>?) ?: return null

    return entriesList.lastOrNull {
      val effDate = Instant.parse(it["effectiveDate"] as String)
      now == effDate || effDate.isBefore(now)
    }
  }

  private fun addColValuesAsParams(
    stmt: PreparedStatement,
    i: Int,
    colCount: Int,
    cols: Map<String, Column>,
    jsonRoot: Any?,
    jsonObject: Any?,
    config: Configuration
  ): Int {
    var n = i
    for (col in cols) {
      val setter = col.value.paramSetterForColumn(
        stmt,
        jsonRoot,
        jsonObject,
        config,
        json
      )
      setter(n)
      setter(n + colCount)

      n++
    }
    return n
  }

  private fun typeToMysql(type: String): String = when (type) {
    "uuid" -> "binary(16)"
    else -> type
  }
}