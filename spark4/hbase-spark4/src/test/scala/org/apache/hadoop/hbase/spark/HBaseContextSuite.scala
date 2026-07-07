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
package org.apache.hadoop.hbase.spark

import org.apache.hadoop.hbase.{CellUtil, HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class HBaseContextSuite extends AnyFunSuite with BeforeAndAfterAll with Logging {

  @transient var sc: SparkContext = _
  var hbaseContext: HBaseContext = _
  val TEST_UTIL = new HBaseTestingUtility

  val tableName = "t1"
  val columnFamily = "c"

  override def beforeAll(): Unit = {
    TEST_UTIL.startMiniCluster()
    logInfo(" - minicluster started")

    TEST_UTIL.createTable(TableName.valueOf(tableName), Bytes.toBytes(columnFamily))
    logInfo(" - created table " + tableName)

    populateTestData()

    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("HBaseContextSuite")
      .set("spark.hadoopRDD.ignoreEmptySplits", "false")
    sc = new SparkContext(sparkConf)

    hbaseContext = new HBaseContext(sc, TEST_UTIL.getConfiguration)
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
    TEST_UTIL.shutdownMiniCluster()
    TEST_UTIL.cleanupTestDir()
  }

  private def populateTestData(): Unit = {
    val connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration)
    val table = connection.getTable(TableName.valueOf(tableName))
    try {
      val puts = Array(
        makePut("row1", "a", "value1"),
        makePut("row2", "a", "value2"),
        makePut("row2", "b", "value2b"),
        makePut("row3", "a", "value3"),
        makePut("row4", "a", "value4"),
        makePut("row5", "a", "value5"))
      puts.foreach(table.put)
    } finally {
      table.close()
      connection.close()
    }
  }

  private def makePut(rowKey: String, qualifier: String, value: String): Put = {
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value))
    put
  }

  test("hbaseRDD with map function transforms scan results") {
    val scan = new Scan()
    val cf = Bytes.toBytes(columnFamily)
    val qual = Bytes.toBytes("a")

    val rdd = hbaseContext.hbaseRDD[String](
      TableName.valueOf(tableName),
      scan,
      (r: (ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result)) => {
        val key = Bytes.toString(r._1.copyBytes())
        val value = Bytes.toString(CellUtil.cloneValue(r._2.getColumnLatestCell(cf, qual)))
        s"$key=$value"
      })

    val results = rdd.collect().sorted
    assert(results.length == 5)
    assert(results.contains("row1=value1"))
    assert(results.contains("row2=value2"))
    assert(results.contains("row3=value3"))
    assert(results.contains("row4=value4"))
    assert(results.contains("row5=value5"))
  }

  test("hbaseRDD raw scan returns correct results with start/stop row") {
    val scan = new Scan()
    scan.withStartRow(Bytes.toBytes("row2"))
    scan.withStopRow(Bytes.toBytes("row4"))

    val rdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)

    val results = rdd.map(r => Bytes.toString(r._1.copyBytes())).collect().sorted
    assert(results.length == 2)
    assert(results(0) == "row2")
    assert(results(1) == "row3")
  }

  test("hbaseRDD with FirstKeyOnlyFilter limits cells per row") {
    val scan = new Scan()
    scan.withStartRow(Bytes.toBytes("row2"))
    scan.withStopRow(Bytes.toBytes("row3"))
    scan.setFilter(new FirstKeyOnlyFilter())

    val rdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)

    val cellCount = rdd.map(r => r._2.listCells().size()).collect().sum
    // row2 has 2 cells (a, b) but FirstKeyOnlyFilter returns only 1 per row
    assert(cellCount == 1)
  }

  test("hbaseRDDAsRows extracts specified columns into Row objects") {
    val scan = new Scan()
    scan.withStartRow(Bytes.toBytes("row1"))
    scan.withStopRow(Bytes.toBytes("row2"))

    val rdd = hbaseContext.hbaseRDDAsRows(tableName, scan, Seq("a"))

    val rows = rdd.collect()
    assert(rows.length == 1)
    assert(rows(0).getString(0) == "value1")
  }

  test("hbaseRDDAsRows returns null for missing columns") {
    val scan = new Scan()
    scan.withStartRow(Bytes.toBytes("row1"))
    scan.withStopRow(Bytes.toBytes("row2"))

    val rdd = hbaseContext.hbaseRDDAsRows(tableName, scan, Seq("a", "nonexistent"))

    val rows = rdd.collect()
    assert(rows.length == 1)
    assert(rows(0).getString(0) == "value1")
    assert(rows(0).get(1) == null)
  }
}
