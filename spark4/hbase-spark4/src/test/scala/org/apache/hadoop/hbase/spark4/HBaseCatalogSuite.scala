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

package org.apache.hadoop.hbase.spark4

import org.apache.hadoop.hbase.spark4.datasources.{DataTypeParserWrapper, DoubleSerDes, HBaseTableCatalog}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HBaseCatalogSuite
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Logging {

  val mapTp = """MAP<int, struct<varchar:string>>"""
  val arrayTp = """array<struct<tinYint:tinyint>>"""
  val arrayMapTp = """MAp<int, ARRAY<double>>"""
  val catalog: String = s"""{
                    |"table":{"namespace":"default", "name":"htable"},
                    |"rowkey":"key1:key2",
                    |"columns":{
                    |"col1":{"cf":"rowkey", "col":"key1", "type":"string"},
                    |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
                    |"col3":{"cf":"cf1", "col":"col2", "type":"binary"},
                    |"col4":{"cf":"cf1", "col":"col3", "type":"timestamp"},
                    |"col5":{"cf":"cf1", "col":"col4", "type":"double", "serdes":"${classOf[DoubleSerDes].getName}"},
                    |"col6":{"cf":"cf1", "col":"col5", "type":"$mapTp"},
                    |"col7":{"cf":"cf1", "col":"col6", "type":"$arrayTp"},
                    |"col8":{"cf":"cf1", "col":"col7", "type":"$arrayMapTp"},
                    |"col9":{"cf":"cf1", "col":"col8", "type":"date"},
                    |"col10":{"cf":"cf1", "col":"col9", "type":"timestamp"}
                    |}
                    |}""".stripMargin

  private val parameters = Map(HBaseTableCatalog.tableCatalog -> catalog)

  lazy val tbl: HBaseTableCatalog = HBaseTableCatalog(parameters)

  private def checkDataType(dataTypeString: String, expectedDataType: DataType): Unit = {
    test(s"parse ${dataTypeString.replace("\n", "")}") {
      assert(DataTypeParserWrapper.parse(dataTypeString) === expectedDataType)
    }
  }

  test("basic") {
    assert(tbl.getField("col1").isRowKey === true)
    assert(tbl.getPrimaryKey === "key1")
    assert(tbl.getField("col3").dt === BinaryType)
    assert(tbl.getField("col4").dt === TimestampType)
    assert(tbl.getField("col5").dt === DoubleType)
    assert(tbl.getField("col5").serdes.isDefined)
    assert(tbl.getField("col4").serdes.isEmpty)
    assert(tbl.getField("col1").isRowKey)
    assert(tbl.getField("col2").isRowKey)
    assert(!tbl.getField("col3").isRowKey)
    assert(tbl.getField("col2").length === Bytes.SIZEOF_DOUBLE)
    assert(tbl.getField("col1").length === -1)
    assert(tbl.getField("col8").length === -1)
    assert(tbl.getField("col9").dt === DateType)
    assert(tbl.getField("col10").dt === TimestampType)
  }

  checkDataType(mapTp, tbl.getField("col6").dt)
  checkDataType(arrayTp, tbl.getField("col7").dt)

  checkDataType(arrayMapTp, tbl.getField("col8").dt)

  test("convert") {
    val m = Map(
      "hbase.columns.mapping" ->
        "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD DOUBLE c:b, C_FIELD BINARY c:c,",
      "hbase.table" -> "NAMESPACE:TABLE")
    val out = HBaseTableCatalog.convert(m)
    val json = out(HBaseTableCatalog.tableCatalog)
    val p = Map(HBaseTableCatalog.tableCatalog -> json)
    val ht = HBaseTableCatalog(p)
    assert(ht.namespace === "NAMESPACE")
    assert(ht.name === "TABLE")
    assert(ht.getField("KEY_FIELD").isRowKey)
    assert(DataTypeParserWrapper.parse("STRING") === ht.getField("A_FIELD").dt)
    assert(!ht.getField("A_FIELD").isRowKey)
    assert(DataTypeParserWrapper.parse("DOUBLE") === ht.getField("B_FIELD").dt)
    assert(DataTypeParserWrapper.parse("BINARY") === ht.getField("C_FIELD").dt)
  }

  test("compatibility") {
    val m = Map(
      "hbase.columns.mapping" ->
        "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD DOUBLE c:b, C_FIELD BINARY c:c,",
      "hbase.table" -> "t1")
    val ht = HBaseTableCatalog(m)
    assert(ht.namespace === "default")
    assert(ht.name === "t1")
    assert(ht.getField("KEY_FIELD").isRowKey)
    assert(DataTypeParserWrapper.parse("STRING") === ht.getField("A_FIELD").dt)
    assert(!ht.getField("A_FIELD").isRowKey)
    assert(DataTypeParserWrapper.parse("DOUBLE") === ht.getField("B_FIELD").dt)
    assert(DataTypeParserWrapper.parse("BINARY") === ht.getField("C_FIELD").dt)
  }
}
