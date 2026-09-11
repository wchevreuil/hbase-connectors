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

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.yetus.audience.InterfaceAudience

/**
 * This is a new class in the spark4 module. Implements InputPartition for serialization of the partition
 * information to be sent to executors.
 *
 * Ranges are executed as HBase Scan operations; points as batched Get operations.
 * This mirrors the spark3 HBaseScanPartition behavior.
 */
@InterfaceAudience.Private
case class HBaseInputPartition(
    index: Int,
    scanRanges: Seq[Range],
    points: Seq[Array[Byte]],
    serverHostname: Option[String] = None)
    extends InputPartition {
  override def preferredLocations(): Array[String] =
    serverHostname.toArray
}
