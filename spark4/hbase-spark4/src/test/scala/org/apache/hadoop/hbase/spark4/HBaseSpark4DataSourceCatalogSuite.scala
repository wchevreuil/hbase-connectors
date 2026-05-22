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
 * distributed under this License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.spark4

import org.apache.hadoop.hbase.spark4.datasources.HBaseTableCatalog
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession

/**
 * Validates that Spark 4 resolves [[DefaultSource]] with a JSON table catalog and reports the catalog
 * schema without running a scan (no live HBase required).
 */
class HBaseSpark4DataSourceCatalogSuite
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var spark: SparkSession = _

  private val catalogJson: String = s"""{
      |"table":{"namespace":"default", "name":"htable"},
      |"rowkey":"key1:key2",
      |"columns":{
      |"col1":{"cf":"rowkey", "col":"key1", "type":"string"},
      |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
      |"col3":{"cf":"cf1", "col":"col2", "type":"binary"}
      |}
      |}""".stripMargin

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("HBaseSpark4DataSourceCatalogSuite")
      .getOrCreate()
  }

  override protected def afterAll(): Unit =
    Option(spark).foreach { s =>
      s.stop(); spark = null
    }

  test("resolve DefaultSource relation schema from catalog (no scan)") {
    val params =
      Map(HBaseTableCatalog.tableCatalog -> catalogJson)
    val expected = HBaseTableCatalog(params).toDataType

    val df = spark.read
      .format(classOf[DefaultSource].getName)
      .option(HBaseTableCatalog.tableCatalog, catalogJson)
      .load()

    df.schema shouldEqual expected
  }
}
