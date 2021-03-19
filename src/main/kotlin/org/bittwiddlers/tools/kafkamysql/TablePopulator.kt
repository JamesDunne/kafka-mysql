package org.bittwiddlers.tools.kafkamysql

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.streams.processor.Processor
import org.apache.kafka.streams.processor.ProcessorContext
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.SQLSyntaxErrorException
import java.time.Instant
import javax.script.ScriptEngine
import javax.sql.DataSource

class TablePopulator(
  val ds: DataSource,
  val json: ObjectMapper,
  val scriptEngine: ScriptEngine,
  val table: Map.Entry<String, Table>
) : Processor<String, JsonObjectGeneral>, AutoCloseable {
  private var now: Instant = Instant.now()
  var colCount: Int = 0

  lateinit var stmtInsert: PreparedStatement
  var primaryKeyColumns: List<Column>? = null
  var deleteWhereNotInColumn: Column? = null
  var deleteWhere: List<Column>? = null

  lateinit var conn: Connection

  fun createTable() {
    val sql = StringBuilder()
    sql.append("create table if not exists ${table.key} (\n")

    val cols = mutableListOf<String>()
    val pkNames = mutableListOf<String>()

    val mapped = table.value.mapped
    val flatMapped = table.value.flatMapped

    when {
      mapped != null -> {
        for (col in mapped.columns ?: emptyMap()) {
          if (col.value.primaryKey) {
            pkNames.add("`${col.key}`")
          }
          cols.add("`${col.key}` ${typeToMysql(col.value.type!!)}")
        }
      }
      flatMapped != null -> {
        for (col in flatMapped.columns ?: emptyMap()) {
          if (col.value.primaryKey) {
            pkNames.add("`${col.key}`")
          }
          cols.add("`${col.key}` ${typeToMysql(col.value.type!!)}")
        }
      }
      else -> throw RuntimeException("Table mapping ${table.key} must have either `mapped` or `flatMapped` section defined")
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
    val pkCols = mutableListOf<Column>()

    colCount = 0

    val mapped = table.value.mapped
    val flatMapped = table.value.flatMapped

    if (flatMapped != null) {
      val columns = flatMapped.columns ?: emptyMap()
      for (col in columns) {
        colsInsName.add("`${col.key}`")
        val param = col.value.paramExpression()
        colsInsValue.add(param)
        val checkParam = "`${col.key}`=${param}"
        colsUpdExpr.add(checkParam)
        // track primaryKey columns for delete statement:
        if (col.value.primaryKey) {
          pkCols.add(col.value)
        }

        colCount++
      }

      val delete = flatMapped.delete
      if (delete != null) {
        val where = delete.where ?: throw RuntimeException("delete.where must be a set of column names")
        deleteWhere = where.map { columns[it] ?: throw RuntimeException("delete.where must be a set of column names; $it is missing") }

        val whereNotIn = delete.whereNotIn ?: throw RuntimeException("delete.whereNotIn must be a valid column name")
        val col = columns[whereNotIn] ?: throw RuntimeException("delete.whereNotIn must be a valid column name")
        deleteWhereNotInColumn = col
      }
    } else if (mapped != null) {
      for (col in mapped.columns ?: emptyMap()) {
        colsInsName.add("`${col.key}`")
        val param = col.value.paramExpression()
        colsInsValue.add(param)
        val checkParam = "`${col.key}`=${param}"
        colsUpdExpr.add(checkParam)

        colCount++
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
  }

  override fun process(key: String, jsonRoot: JsonObjectGeneral) {
    val flatMapped = table.value.flatMapped
    val mapped = table.value.mapped
    if (flatMapped != null) {
      processFlatMapped(flatMapped, jsonRoot)
    } else if (mapped != null) {
      processMapped(mapped, jsonRoot)
    }
  }

  private fun processMapped(mapped: MappedTable, jsonRoot: JsonObjectGeneral) {
    try {
      var n = 1
      stmtInsert.clearParameters()

      addColValuesAsParams(
        stmtInsert,
        n,
        colCount,
        mapped.columns ?: emptyMap(),
        jsonRoot,
        jsonRoot
      )

      stmtInsert.executeUpdate()
      conn.commit()
    } catch (ex: SQLException) {
      conn.rollback()
      throw ex
    }
  }

  private fun processFlatMapped(flatMapped: FlatMappedTable, jsonRoot: JsonObjectGeneral) {
    val entityObjects = flatMapped.fromSelector?.apply(jsonRoot) ?: throw RuntimeException("missing `from` flatMap function")

    // start a batch:
    stmtInsert.clearBatch()

    loop@ for (entityObj in entityObjects) {
      stmtInsert.clearParameters()

      var n = 1
      n = addColValuesAsParams(
        stmtInsert,
        n,
        colCount,
        flatMapped.columns ?: emptyMap(),
        jsonRoot,
        entityObj
      )

      stmtInsert.addBatch()
    }

    try {
      // execute the UPSERT batch:
      stmtInsert.executeBatch()

      val delWhere = deleteWhere
      if (delWhere != null) {
        val delCol = deleteWhereNotInColumn ?: throw RuntimeException("flatMapped.delete must have whereNotIn defined")

        // delete entities not in the alive list:
        val whereClauseExprs = mutableListOf<String>()
        whereClauseExprs.addAll(delWhere.map(Column::nameEqualsParamExpression) ?: emptyList())
        if (entityObjects.isNotEmpty()) {
          whereClauseExprs.add("`${delCol.columnName}` not in (${
            entityObjects.joinToString { delCol.paramExpression() }
          })")
        }

        val delSql = "delete from `${table.key}` where ${whereClauseExprs.joinToString(" and ")}"
        conn.prepareStatement(delSql).use {
          // set the parameters for the where clause:
          var n = 1
          for (col in delWhere) {
            col.paramSetterForColumn(
              it,
              jsonRoot,
              jsonRoot,
              json
            )(n)
            n++
          }
          for (entityObj in entityObjects) {
            delCol.paramSetterForColumn(
              it,
              jsonRoot,
              entityObj,
              json
            )(n)
            n++
          }

          // execute the delete:
          try {
            it.executeUpdate()
          } catch (ex: SQLSyntaxErrorException) {
            throw RuntimeException("SQL syntax error in query:\n$delSql", ex)
          }
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

  private fun addColValuesAsParams(
    stmt: PreparedStatement,
    i: Int,
    colCount: Int,
    cols: Map<String, Column>,
    jsonRoot: JsonObjectGeneral?,
    jsonObject: JsonObjectGeneral?
  ): Int {
    var n = i
    for (col in cols) {
      val setter = col.value.paramSetterForColumn(
        stmt,
        jsonRoot,
        jsonObject,
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