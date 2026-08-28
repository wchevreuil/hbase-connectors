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

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class HBaseScanBuilderSuite extends AnyFunSuite {

  val catalog = s"""{
                    |"table":{"namespace":"default", "name":"testtable"},
                    |"rowkey":"key",
                    |"columns":{
                    |"key":{"cf":"rowkey", "col":"key", "type":"string"},
                    |"name":{"cf":"cf", "col":"name", "type":"string"},
                    |"age":{"cf":"cf", "col":"age", "type":"int"},
                    |"salary":{"cf":"cf", "col":"salary", "type":"double"}
                    |}
                    |}""".stripMargin

  val schema = StructType(Seq(
    StructField("key", StringType),
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("salary", DoubleType)))

  val properties = Map(HBaseTableCatalog.tableCatalog -> catalog)

  private def newBuilder(): HBaseScanBuilder = new HBaseScanBuilder(schema, properties)

  test("EqualTo on known column is pushed") {
    val builder = newBuilder()
    val unsupported = builder.pushFilters(Array(EqualTo("name", "Alice")))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
    assert(builder.pushedFilters()(0) == EqualTo("name", "Alice"))
  }

  test("LessThan on known column is pushed") {
    val builder = newBuilder()
    val unsupported = builder.pushFilters(Array(LessThan("age", 30)))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
  }

  test("GreaterThan on known column is pushed") {
    val builder = newBuilder()
    val unsupported = builder.pushFilters(Array(GreaterThan("salary", 50000.0)))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
  }

  test("LessThanOrEqual on known column is pushed") {
    val builder = newBuilder()
    val unsupported = builder.pushFilters(Array(LessThanOrEqual("age", 25)))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
  }

  test("GreaterThanOrEqual on known column is pushed") {
    val builder = newBuilder()
    val unsupported = builder.pushFilters(Array(GreaterThanOrEqual("age", 18)))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
  }

  test("StringStartsWith on known column is pushed") {
    val builder = newBuilder()
    val unsupported = builder.pushFilters(Array(StringStartsWith("name", "A")))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
  }

  test("IsNull on known column is pushed") {
    val builder = newBuilder()
    val unsupported = builder.pushFilters(Array(IsNull("name")))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
  }

  test("IsNotNull on known column is pushed") {
    val builder = newBuilder()
    val unsupported = builder.pushFilters(Array(IsNotNull("name")))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
  }

  test("filter on unknown column is not pushed") {
    val builder = newBuilder()
    val unsupported = builder.pushFilters(Array(EqualTo("unknown_col", "value")))
    assert(unsupported.length == 1)
    assert(builder.pushedFilters().isEmpty)
  }

  test("Or compound filter is pushed") {
    val builder = newBuilder()
    val filter = Or(EqualTo("name", "Alice"), EqualTo("name", "Bob"))
    val unsupported = builder.pushFilters(Array(filter))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
  }

  test("And compound filter is pushed") {
    val builder = newBuilder()
    val filter = And(GreaterThan("age", 20), LessThan("age", 40))
    val unsupported = builder.pushFilters(Array(filter))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
  }

  test("mixed supported and unsupported filters are separated") {
    val builder = newBuilder()
    val filters = Array[Filter](
      EqualTo("name", "Alice"),
      EqualTo("unknown", "value"),
      GreaterThan("age", 20))
    val unsupported = builder.pushFilters(filters)
    assert(unsupported.length == 1)
    assert(unsupported(0) == EqualTo("unknown", "value"))
    assert(builder.pushedFilters().length == 2)
  }

  test("filter on row key column is pushed") {
    val builder = newBuilder()
    val unsupported = builder.pushFilters(Array(EqualTo("key", "row001")))
    assert(unsupported.isEmpty)
    assert(builder.pushedFilters().length == 1)
  }

  test("pruneColumns reduces required schema") {
    val builder = newBuilder()
    val pruned = StructType(Seq(StructField("key", StringType), StructField("name", StringType)))
    builder.pruneColumns(pruned)
    val scan = builder.build()
    assert(scan.readSchema() == pruned)
  }

  test("multiple pushFilters calls replace previous filters") {
    val builder = newBuilder()
    builder.pushFilters(Array(EqualTo("name", "Alice")))
    assert(builder.pushedFilters().length == 1)
    builder.pushFilters(Array(GreaterThan("age", 20), LessThan("age", 40)))
    assert(builder.pushedFilters().length == 2)
  }
}
