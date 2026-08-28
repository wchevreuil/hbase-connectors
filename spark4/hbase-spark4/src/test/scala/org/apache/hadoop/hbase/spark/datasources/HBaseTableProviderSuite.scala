/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.spark.datasources

import java.io.{File, FileOutputStream}
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.spark.Logging
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class HBaseTableProviderSuite extends AnyFunSuite with BeforeAndAfterAll with Logging {

  val TEST_UTIL = new HBaseTestingUtility
  var spark: SparkSession = _
  var configFile: File = _

  val tableName = "test_provider"
  val columnFamily = "cf"
  val numRows = 20

  val catalog: String = s"""{
    |"table":{"namespace":"default", "name":"$tableName"},
    |"rowkey":"key",
    |"columns":{
    |"key":{"cf":"rowkey", "col":"key", "type":"string"},
    |"name":{"cf":"$columnFamily", "col":"name", "type":"string"},
    |"age":{"cf":"$columnFamily", "col":"age", "type":"string"},
    |"salary":{"cf":"$columnFamily", "col":"salary", "type":"string"}
    |}
    |}""".stripMargin

  override def beforeAll(): Unit = {
    TEST_UTIL.startMiniCluster()
    logInfo(" - minicluster started")

    TEST_UTIL.createTable(TableName.valueOf(tableName), Bytes.toBytes(columnFamily))
    logInfo(s" - created table $tableName")

    populateTestData()

    val tmpDir = new File("target", "test-tmp")
    tmpDir.mkdirs()
    configFile = File.createTempFile("hbase-site", ".xml", tmpDir)
    configFile.deleteOnExit()
    val out = new FileOutputStream(configFile)
    TEST_UTIL.getConfiguration.writeXml(out)
    out.close()

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("HBaseTableProviderSuite")
      .config("spark.hadoopRDD.ignoreEmptySplits", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (configFile != null) {
      configFile.delete()
    }
    TEST_UTIL.shutdownMiniCluster()
    TEST_UTIL.cleanupTestDir()
  }

  private def populateTestData(): Unit = {
    val connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration)
    val table = connection.getTable(TableName.valueOf(tableName))
    try {
      for (i <- 0 until numRows) {
        val key = f"row$i%03d"
        val put = new Put(Bytes.toBytes(key))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name"), Bytes.toBytes(s"Name$i"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("age"), Bytes.toBytes(s"${20 + i}"))
        put.addColumn(
          Bytes.toBytes(columnFamily),
          Bytes.toBytes("salary"),
          Bytes.toBytes(s"${30000 + i * 1000}"))
        table.put(put)
      }
    } finally {
      table.close()
      connection.close()
    }
  }

  private def loadTable() = {
    spark.read
      .format("org.apache.hadoop.hbase.spark.datasources.HBaseTableProvider")
      .option("catalog", catalog)
      .option(HBaseSparkConf.HBASE_CONFIG_LOCATION, configFile.getAbsolutePath)
      .load()
  }

  test("full table scan returns all rows") {
    val df = loadTable()
    assert(df.count() == numRows)
  }

  test("select subset of columns") {
    val df = loadTable().select("key", "name")
    assert(df.columns.length == 2)
    assert(df.count() == numRows)
    val firstRow = df.orderBy("key").first()
    assert(firstRow.getString(0) == "row000")
    assert(firstRow.getString(1) == "Name0")
  }

  test("filter with EqualTo on non-row-key column") {
    val df = loadTable().filter("name = 'Name5'")
    assert(df.count() == 1)
    val row = df.first()
    assert(row.getAs[String]("key") == "row005")
  }

  test("filter with row key EqualTo narrows scan") {
    val df = loadTable().filter("key = 'row010'")
    assert(df.count() == 1)
    val row = df.first()
    assert(row.getAs[String]("name") == "Name10")
  }

  test("filter with row key range predicates") {
    val df = loadTable().filter("key >= 'row005' AND key < 'row010'")
    assert(df.count() == 5)
  }

  test("count with filter on non-output columns") {
    val df = loadTable().filter("name = 'Name3' AND age = '23'")
    assert(df.count() == 1)
  }

  test("StringStartsWith filter") {
    val df = loadTable().filter("name LIKE 'Name1%'")
    assert(df.count() == 11)
  }

  test("SQL query via temp view") {
    val df = loadTable()
    df.createOrReplaceTempView("hbase_test")
    val result = spark.sql("SELECT COUNT(*) as cnt FROM hbase_test WHERE age = '25'")
    assert(result.first().getLong(0) == 1)
  }

  test("select all columns preserves schema") {
    val df = loadTable()
    assert(df.schema.fieldNames.sorted.sameElements(Array("age", "key", "name", "salary")))
  }

  test("empty result for non-matching filter") {
    val df = loadTable().filter("name = 'NonExistent'")
    assert(df.count() == 0)
  }
}
