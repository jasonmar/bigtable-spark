/*
 *  Copyright 2018 Google
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.google.cloud.example


import com.google.bigtable.v2.{MutateRowsRequest, MutateRowsResponse}
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.{BigtableDataClient, BigtableSession}
import com.google.protobuf.ByteString
import org.apache.spark.rdd.RDD

object BigTable {

  // Single session per JVM
  @volatile private var sharedSession: BigtableSession = _

  def getSharedClient(projectId: String, instanceId: String): BigtableDataClient = {
    synchronized {
      if (sharedSession == null) {
        val opts = new BigtableOptions.Builder()
          .setProjectId(projectId)
          .setInstanceId(instanceId)
          .setUserAgent("Spark")
          .build()
        sharedSession = new BigtableSession(opts)
      }
    }
    sharedSession.getDataClient
  }

  def buildMutateRowsRequest(rows: Seq[(String,Writeable)], table: String): MutateRowsRequest = {
    val b = MutateRowsRequest.newBuilder().setTableName(table)
    rows.foreach{t =>
      b.addEntriesBuilder()
        .setRowKey(ByteString.copyFromUtf8(t._1))
        .addMutationsBuilder()
        .getSetCellBuilder
        .setFamilyName(t._2.family)
        .setColumnQualifier(ByteString.copyFromUtf8(t._2.column))
        .setValue(ByteString.copyFrom(t._2.bytes))
    }
    b.build()
  }

  /** Contains bytes to be written to a specific column and column family */
  case class Writeable(bytes: Array[Byte], family: String, column: String)

  class Writer(projectId: String, instanceId: String, table: String, batchSize: Int) extends Serializable {
    @transient private val client: BigtableDataClient =
      getSharedClient(projectId, instanceId)

    def write(rows: Iterator[(String,Writeable)]): Iterator[MutateRowsResponse] = {
      import scala.collection.JavaConverters.iterableAsScalaIterableConverter
      rows.grouped(batchSize)
        .map(batch => client.mutateRows(buildMutateRowsRequest(batch, table)))
        .flatMap(_.asScala)
    }
  }

  /** Write rows to the specified table
    * Each row is a tuple containing Row Key and a Writeable
    */
  def write(project: String,
            instanceId: String,
            table: String,
            batchSize: Int,
            rows: RDD[(String, Writeable)]): RDD[MutateRowsResponse] = {
    rows.mapPartitions{rows =>
      new BigTable.Writer(project, instanceId, table, batchSize)
        .write(rows)
    }
  }
}
