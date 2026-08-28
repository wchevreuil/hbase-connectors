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
import org.scalatest.funsuite.AnyFunSuite

class ScanRangeSuite extends AnyFunSuite {

  private def toBytes(s: String): Array[Byte] = Bytes.toBytes(s)

  test("ScanRange mergeIntersect narrows bounds") {
    val r1 = new ScanRange(toBytes("F"), true, toBytes("A"), true)
    val r2 = new ScanRange(toBytes("D"), true, toBytes("C"), true)
    r1.mergeIntersect(r2)
    assert(Bytes.toString(r1.upperBound) == "D")
    assert(Bytes.toString(r1.lowerBound) == "C")
    assert(r1.isUpperBoundEqualTo)
    assert(r1.isLowerBoundEqualTo)
  }

  test("ScanRange mergeIntersect with exclusive bounds") {
    val r1 = new ScanRange(toBytes("F"), true, toBytes("A"), true)
    val r2 = new ScanRange(toBytes("F"), false, toBytes("A"), false)
    r1.mergeIntersect(r2)
    assert(!r1.isUpperBoundEqualTo)
    assert(!r1.isLowerBoundEqualTo)
  }

  test("ScanRange mergeUnion widens bounds") {
    val r1 = new ScanRange(toBytes("D"), true, toBytes("C"), true)
    val r2 = new ScanRange(toBytes("F"), true, toBytes("A"), true)
    r1.mergeUnion(r2)
    assert(Bytes.toString(r1.upperBound) == "F")
    assert(Bytes.toString(r1.lowerBound) == "A")
    assert(r1.isUpperBoundEqualTo)
    assert(r1.isLowerBoundEqualTo)
  }

  test("ScanRange mergeUnion with same bounds picks inclusive") {
    val r1 = new ScanRange(toBytes("D"), false, toBytes("A"), false)
    val r2 = new ScanRange(toBytes("D"), true, toBytes("A"), true)
    r1.mergeUnion(r2)
    assert(r1.isUpperBoundEqualTo)
    assert(r1.isLowerBoundEqualTo)
  }

  test("ScanRange containsPoint with inclusive bounds") {
    val r = new ScanRange(toBytes("D"), true, toBytes("A"), true)
    assert(r.containsPoint(toBytes("A")))
    assert(r.containsPoint(toBytes("B")))
    assert(r.containsPoint(toBytes("D")))
    assert(!r.containsPoint(toBytes("E")))
  }

  test("ScanRange containsPoint with exclusive bounds") {
    val r = new ScanRange(toBytes("D"), false, toBytes("A"), false)
    assert(!r.containsPoint(toBytes("A")))
    assert(r.containsPoint(toBytes("B")))
    assert(!r.containsPoint(toBytes("D")))
  }

  test("ScanRange getOverLapScanRange returns overlap") {
    val r1 = new ScanRange(toBytes("D"), true, toBytes("A"), true)
    val r2 = new ScanRange(toBytes("F"), true, toBytes("C"), true)
    val overlap = r1.getOverLapScanRange(r2)
    assert(overlap != null)
    assert(Bytes.toString(overlap.lowerBound) == "C")
    assert(Bytes.toString(overlap.upperBound) == "D")
  }

  test("ScanRange getOverLapScanRange returns null for no overlap") {
    val r1 = new ScanRange(toBytes("B"), true, toBytes("A"), true)
    val r2 = new ScanRange(toBytes("F"), true, toBytes("D"), true)
    val overlap = r1.getOverLapScanRange(r2)
    assert(overlap == null)
  }

  test("ScanRange with null upper bound treated as unbounded") {
    val r1 = new ScanRange(null, true, toBytes("A"), true)
    val r2 = new ScanRange(toBytes("Z"), true, toBytes("A"), true)
    r1.mergeIntersect(r2)
    assert(Bytes.toString(r1.upperBound) == "Z")
  }

  test("ScanRange with empty lower bound treated as unbounded") {
    val r1 = new ScanRange(toBytes("Z"), true, new Array[Byte](0), true)
    val r2 = new ScanRange(toBytes("Z"), true, toBytes("A"), true)
    r1.mergeIntersect(r2)
    assert(Bytes.toString(r1.lowerBound) == "A")
  }

  test("RowKeyFilter mergeIntersect combines ranges") {
    val f1 = new RowKeyFilter(null, new ScanRange(toBytes("F"), true, toBytes("A"), true))
    val f2 = new RowKeyFilter(null, new ScanRange(toBytes("D"), true, toBytes("C"), true))
    f1.mergeIntersect(f2)
    assert(f1.ranges.nonEmpty)
  }

  test("RowKeyFilter mergeUnion accumulates points") {
    val f1 = new RowKeyFilter(toBytes("A"), null)
    val f2 = new RowKeyFilter(toBytes("B"), null)
    f1.mergeUnion(f2)
    assert(f1.points.size == 2)
    assert(f1.points.exists(Bytes.equals(_, toBytes("A"))))
    assert(f1.points.exists(Bytes.equals(_, toBytes("B"))))
  }

  test("RowKeyFilter with point only") {
    val f = new RowKeyFilter(toBytes("key1"), null)
    assert(f.points.size == 1)
    assert(f.ranges.isEmpty)
  }

  test("RowKeyFilter with range only") {
    val f = new RowKeyFilter(null, new ScanRange(toBytes("Z"), true, toBytes("A"), true))
    assert(f.points.isEmpty)
    assert(f.ranges.size == 1)
  }

  test("RowKeyFilter default has empty range covering all") {
    val f = new RowKeyFilter()
    assert(f.ranges.size == 1)
    assert(f.ranges.head.lowerBound.sameElements(new Array[Byte](0)))
    assert(f.ranges.head.upperBound == null)
  }
}
