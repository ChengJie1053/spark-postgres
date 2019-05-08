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

import java.io.File
import java.sql.DriverManager
import java.util.TimeZone

import io.airlift.testing.postgresql.TestingPostgreSqlServer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class GreenplumUtilsSuite extends SparkFunSuite {
  val timeZoneId: String = TimeZone.getDefault.getID

  var postgres: TestingPostgreSqlServer = _
  var url: String = _
  var sparkSession: SparkSession = _
  var tempDir: File = _

  override def beforeAll(): Unit = {
    tempDir = Utils.createTempDir()
    postgres = new TestingPostgreSqlServer("gptest", "gptest")
    url = postgres.getJdbcUrl
    sparkSession = SparkSession.builder()
      .config("spark.master", "local")
      .config("spark.app.name", "testGp")
      .config("spark.sql.warehouse.dir", s"${tempDir.getAbsolutePath}/warehouse")
      .config("spark.local.dir", s"${tempDir.getAbsolutePath}/local")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (postgres != null) {
        postgres.close()
      }
      if (sparkSession != null) {
        sparkSession.stop()
      }
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  ignore("make converter") {
    val options = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src")), timeZoneId)

    val row1 = Row(true, 1.toByte, 2.toShort, 3, 4.toLong,
      5.toFloat, 6.toDouble, 7.toString, 8.toString.getBytes,
      9,
      10L,
      new java.math.BigDecimal(11),
      Array[String]("12", "12"),
      Map(13 -> 13, 130 -> 130),
      Row(14, "15"))

    val row2 = Row(null)

    val boolConverter = GreenplumUtils.makeConverter(BooleanType, options)
    assert(boolConverter.apply(row1, 0) === "true")
    intercept[NullPointerException](boolConverter.apply(row2, 0) === "")

    val byteConverter = GreenplumUtils.makeConverter(ByteType, options)
    assert(byteConverter(row1, 1) === "1")

    val shortConverter = GreenplumUtils.makeConverter(ShortType, options)
    assert(shortConverter(row1, 2) === "2")

    val intConverter = GreenplumUtils.makeConverter(IntegerType, options)
    assert(intConverter(row1, 3) === "3")

    val longConverter = GreenplumUtils.makeConverter(LongType, options)
    assert(longConverter(row1, 4) === "4")

    val floatConverter = GreenplumUtils.makeConverter(FloatType, options)
    assert(floatConverter(row1, 5) === "5.0")

    val doubleConverter = GreenplumUtils.makeConverter(DoubleType, options)
    assert(doubleConverter(row1, 6) === "6.0")

    val strConverter = GreenplumUtils.makeConverter(StringType, options)
    assert(strConverter(row1, 7) === "7")

    val binConverter = GreenplumUtils.makeConverter(BinaryType, options)
    assert(binConverter(row1, 8) === "8")

    val dateConverter = GreenplumUtils.makeConverter(DateType, options)
    assert(dateConverter(row1, 9) === options.dateFormat.format(DateTimeUtils.toJavaDate(9)))

    val tsConverter = GreenplumUtils.makeConverter(TimestampType, options)
    assert(tsConverter(row1, 10) ===
      options.timestampFormat.format(DateTimeUtils.toJavaTimestamp(10)))

    val decimalConverter = GreenplumUtils.makeConverter(DecimalType(2, 0), options)
    assert(decimalConverter(row1, 11) === new java.math.BigDecimal(11).toString)

    val arrConverter = GreenplumUtils.makeConverter(ArrayType(StringType), options)
    assert(arrConverter(row1, 12) === Array[String]("12", "12").mkString("[", ",", "]"))

    val mapConverter = GreenplumUtils.makeConverter(MapType(IntegerType, IntegerType), options)
    assert(mapConverter(row1, 13) ===
      Map(13 -> 13, 130 ->130)
        .map(e => e._1 + ":" + e._2).toSeq.sorted.mkString("{", ",", "}"))

    val structConverter =
      GreenplumUtils.makeConverter(
        StructType(Array(StructField("a", IntegerType), StructField("b", StringType))), options)
    assert(structConverter(row1, 14) === "{\"a\":14,\"b\":15}")
  }

  test("test copy to greenplum with postgres") {
    // scalastyle:off
    val kvs = Map[Int, String](0 -> " ", 1 -> "\t", 2 -> "\n", 3 -> "\r", 4 -> "\\t",
      5 -> "\\n", 6 -> "\\", 7 -> ",", 8 -> "te\tst", 9 -> "1`'`", 10 -> "中文测试")
    // scalastyle:on
    val rdd = sparkSession.sparkContext.parallelize(kvs.toSeq)
    val df = sparkSession.createDataFrame(rdd)
    val conn = DriverManager.getConnection(url)
    val tblname = "gptbl"
    try {
      val stat1 = conn.createStatement()
      stat1.execute(s"create table $tblname(_1 Int, _2 text)")
      val schema = new StructType().add("_1", IntegerType).add("_2", StringType)

      val parameters = CaseInsensitiveMap(Map("url" -> s"$url", "dbtable" -> s"$tblname"))
      GreenplumUtils.copyToGreenplum(df, schema, GreenplumOptions(parameters, timeZoneId))

      val stat2 = conn.createStatement()
      stat2.executeQuery(s"select * from $tblname")
      stat2.setFetchSize(kvs.size)
      val result = stat2.getResultSet
      while (result.next()) {
        val k = result.getInt(1)
        val v = result.getString(2)
        assert(kvs.get(k).get === v)
      }
    } finally {
      conn.close()
    }
  }
}