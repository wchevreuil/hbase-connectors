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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class HBaseTableCatalogSuite extends AnyFunSuite {

  val map = s"""MAP<int, struct<varchar:string>>"""
  val array = s"""array<struct<tinYint:tinyint>>"""
  val arrayMap = s"""MAp<int, ARRAY<double>>"""

  val catalog = s"""{
                    |"table":{"namespace":"default", "name":"htable"},
                    |"rowkey":"key1:key2",
                    |"columns":{
                    |"col1":{"cf":"rowkey", "col":"key1", "type":"string"},
                    |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
                    |"col3":{"cf":"cf1", "col":"col2", "type":"binary"},
                    |"col4":{"cf":"cf1", "col":"col3", "type":"timestamp"},
                    |"col5":{"cf":"cf1", "col":"col4", "type":"double", "serdes":"${classOf[
                    DoubleSerDes].getName}"},
                    |"col6":{"cf":"cf1", "col":"col5", "type":"$map"},
                    |"col7":{"cf":"cf1", "col":"col6", "type":"$array"},
                    |"col8":{"cf":"cf1", "col":"col7", "type":"$arrayMap"},
                    |"col9":{"cf":"cf1", "col":"col8", "type":"date"},
                    |"col10":{"cf":"cf1", "col":"col9", "type":"timestamp"}
                    |}
                    |}""".stripMargin

  val parameters = Map(HBaseTableCatalog.tableCatalog -> catalog)
  val t = HBaseTableCatalog(parameters)

  test("basic field properties") {
    assert(t.getField("col1").isRowKey)
    assert(t.getPrimaryKey == "key1")
    assert(t.getField("col3").dt == BinaryType)
    assert(t.getField("col4").dt == TimestampType)
    assert(t.getField("col5").dt == DoubleType)
    assert(t.getField("col5").serdes.isDefined)
    assert(t.getField("col4").serdes.isEmpty)
    assert(t.getField("col1").isRowKey)
    assert(t.getField("col2").isRowKey)
    assert(!t.getField("col3").isRowKey)
    assert(t.getField("col2").length == Bytes.SIZEOF_DOUBLE)
    assert(t.getField("col1").length == -1)
    assert(t.getField("col8").length == -1)
    assert(t.getField("col9").dt == DateType)
    assert(t.getField("col10").dt == TimestampType)
  }

  test("composite row key fields") {
    val rowKey = t.getRowKey
    assert(rowKey.length == 2)
    assert(rowKey.head.col == "key1")
    assert(rowKey(1).col == "key2")
  }

  test("namespace and table name") {
    assert(t.namespace == "default")
    assert(t.name == "htable")
  }

  test("column families") {
    val cfs = t.getColumnFamilies
    assert(cfs.contains("cf1"))
    assert(!cfs.contains("rowkey"))
  }

  test("parse MAP type") {
    assert(DataTypeParserWrapper.parse(map) === t.getField("col6").dt)
  }

  test("parse array type") {
    assert(DataTypeParserWrapper.parse(array) === t.getField("col7").dt)
  }

  test("parse array of map type") {
    assert(DataTypeParserWrapper.parse(arrayMap) === t.getField("col8").dt)
  }

  test("convert legacy hbase.columns.mapping format") {
    val m = Map(
      "hbase.columns.mapping" ->
        "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD DOUBLE c:b, C_FIELD BINARY c:c,",
      "hbase.table" -> "NAMESPACE:TABLE")
    val converted = HBaseTableCatalog(m)
    assert(converted.namespace === "NAMESPACE")
    assert(converted.name == "TABLE")
    assert(converted.getField("KEY_FIELD").isRowKey)
    assert(DataTypeParserWrapper.parse("STRING") === converted.getField("A_FIELD").dt)
    assert(!converted.getField("A_FIELD").isRowKey)
    assert(DataTypeParserWrapper.parse("DOUBLE") === converted.getField("B_FIELD").dt)
    assert(DataTypeParserWrapper.parse("BINARY") === converted.getField("C_FIELD").dt)
  }

  test("convert legacy format with default namespace") {
    val m = Map(
      "hbase.columns.mapping" ->
        "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD DOUBLE c:b, C_FIELD BINARY c:c,",
      "hbase.table" -> "t1")
    val converted = HBaseTableCatalog(m)
    assert(converted.namespace === "default")
    assert(converted.name == "t1")
    assert(converted.getField("KEY_FIELD").isRowKey)
  }

  test("single row key field") {
    val simpleCatalog = s"""{
                           |"table":{"namespace":"default", "name":"simple"},
                           |"rowkey":"id",
                           |"columns":{
                           |"id":{"cf":"rowkey", "col":"id", "type":"string"},
                           |"name":{"cf":"cf", "col":"name", "type":"string"}
                           |}
                           |}""".stripMargin
    val params = Map(HBaseTableCatalog.tableCatalog -> simpleCatalog)
    val cat = HBaseTableCatalog(params)
    assert(cat.getRowKey.length == 1)
    assert(cat.getRowKey.head.col == "id")
    assert(cat.getField("id").isRowKey)
    assert(!cat.getField("name").isRowKey)
    assert(cat.getField("name").cf == "cf")
    assert(cat.getField("name").col == "name")
  }

  test("fixed-length row key field has correct length") {
    val intKeyCatalog = s"""{
                           |"table":{"namespace":"default", "name":"intkey"},
                           |"rowkey":"id",
                           |"columns":{
                           |"id":{"cf":"rowkey", "col":"id", "type":"int"},
                           |"value":{"cf":"cf", "col":"v", "type":"string"}
                           |}
                           |}""".stripMargin
    val params = Map(HBaseTableCatalog.tableCatalog -> intKeyCatalog)
    val cat = HBaseTableCatalog(params)
    assert(cat.getRowKey.head.length == Bytes.SIZEOF_INT)
    assert(cat.row.varLength == false)
  }

  test("variable-length row key sets varLength") {
    assert(t.row.varLength == true)
  }

  test("cfBytes and colBytes for row key field") {
    val field = t.getField("col1")
    assert(field.cfBytes.sameElements(Bytes.toBytes("")))
    assert(field.colBytes.sameElements(Bytes.toBytes("key")))
  }

  test("cfBytes and colBytes for regular field") {
    val field = t.getField("col3")
    assert(field.cfBytes.sameElements(Bytes.toBytes("cf1")))
    assert(field.colBytes.sameElements(Bytes.toBytes("col2")))
  }

  test("custom namespace") {
    val nsCatalog = s"""{
                       |"table":{"namespace":"myns", "name":"mytable"},
                       |"rowkey":"k",
                       |"columns":{
                       |"k":{"cf":"rowkey", "col":"k", "type":"string"},
                       |"v":{"cf":"data", "col":"val", "type":"string"}
                       |}
                       |}""".stripMargin
    val params = Map(HBaseTableCatalog.tableCatalog -> nsCatalog)
    val cat = HBaseTableCatalog(params)
    assert(cat.namespace == "myns")
    assert(cat.name == "mytable")
  }
}
