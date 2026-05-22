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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ Result, Scan }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ IdentityTableMapper, TableInputFormat, TableMapReduceUtil }
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SerializableWritable, SparkContext }
import org.apache.yetus.audience.InterfaceAudience
import scala.reflect.ClassTag

/**
 * Narrow Spark 4 port of `org.apache.hadoop.hbase.spark.HBaseContext`: enough for datasource scans
 * via `NewHBaseRDD` / TableInputFormat. Bulk load, streaming, and imperative helpers are intentionally
 * omitted until Phase 1+ expands parity (see SPARK4-ROADMAP.md).
 */
@InterfaceAudience.Public
class HBaseContext(@transient val sc: SparkContext, @transient val config: Configuration)
    extends Serializable
    with Logging {

  @transient protected var tmpHdfsConfiguration: Configuration = config

  {
    val j = Job.getInstance(config)
    TableMapReduceUtil.initCredentials(j)
  }

  val broadcastedConf: Broadcast[SerializableWritable[Configuration]] =
    sc.broadcast(new SerializableWritable(config))

  LatestHBaseContextCache.latest = this

  /** @see [[org.apache.hadoop.hbase.spark.HBaseContext.hbaseRDD(TableName,Scan,(ImmutableBytesWritable,Result)=>U)*]] */
  def hbaseRDD[U: ClassTag](
      tableName: TableName,
      scan: Scan,
      f: ((ImmutableBytesWritable, Result)) => U): RDD[U] = {

    val mapJob = Job.getInstance(getConf(broadcastedConf))
    TableMapReduceUtil.initCredentials(mapJob)
    TableMapReduceUtil.initTableMapperJob(
      tableName,
      scan,
      classOf[IdentityTableMapper],
      null,
      null,
      mapJob)

    val jconf = new JobConf(mapJob.getConfiguration)
    val jobCreds = jconf.getCredentials()
    UserGroupInformation.setConfiguration(sc.hadoopConfiguration)
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())

    new NewHBaseRDD(
      sc,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result],
      mapJob.getConfiguration,
      this).map(f)
  }

  def hbaseRDD(
      tableName: TableName,
      scans: Scan): RDD[(ImmutableBytesWritable, Result)] = {
    hbaseRDD[(ImmutableBytesWritable, Result)](
      tableName,
      scans,
      (r: (ImmutableBytesWritable, Result)) => r)
  }

  /** Visible to datasource / context utilities that clone broadcast config on executors. */
  private[hbase] def getConf(
      configBroadcast: Broadcast[SerializableWritable[Configuration]]): Configuration = {

    if (tmpHdfsConfiguration == null) {
      try {
        tmpHdfsConfiguration = configBroadcast.value.value
      } catch {
        case ex: Exception => logError("Unable to getConfig from broadcast", ex)
      }
    }
    tmpHdfsConfiguration
  }
}

/** Same semantics as Spark 3: last constructed `HBaseContext` wins. */
@InterfaceAudience.Private
object LatestHBaseContextCache {
  var latest: HBaseContext = null
}
