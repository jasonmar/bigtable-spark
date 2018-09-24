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

  /** Add a mutation to a request
    * mutateRowsRequest is expected to have table already set
    * default entry is expected to have family and column already set
    */
  def addMutation(rowKey: String,
                  bytes: Array[Byte],
                  mutateRowsRequest: MutateRowsRequest.Builder,
                  entry: MutateRowsRequest.Entry.Builder): MutateRowsRequest.Builder = {
    entry.setRowKey(ByteString.copyFromUtf8(rowKey))
      .getMutationsBuilder(0)
      .getSetCellBuilder
      .setValue(ByteString.copyFrom(bytes))
      .setTimestampMicros(-1)
    mutateRowsRequest.addEntries(entry)
  }

  class Writer(projectId: String,
               instanceId: String,
               table: String,
               family: String,
               column: String,
               batchSize: Int) extends Serializable {
    @transient private val client: BigtableDataClient =
      getSharedClient(projectId, instanceId)

    val defaultRequest: MutateRowsRequest.Builder =
      MutateRowsRequest.newBuilder()
        .setTableName(s"projects/$projectId/instances/$instanceId/tables/$table")

    val defaultEntry: MutateRowsRequest.Entry.Builder = {
      val entry = MutateRowsRequest.Entry.newBuilder()
      entry.addMutationsBuilder()
        .getSetCellBuilder
        .setFamilyName(family)
        .setColumnQualifier(ByteString.copyFromUtf8(column))
      entry
    }

    def write(rows: Iterator[(String, Array[Byte])]): Iterator[MutateRowsResponse] = {
      import scala.collection.JavaConverters.iterableAsScalaIterableConverter
      rows.grouped(batchSize)
        .map{batch =>
          val mutateRowsRequest = batch.foldLeft(defaultRequest.clone){(builder, row) =>
            addMutation(row._1, row._2, builder, defaultEntry.clone)
          }
          client.mutateRows(mutateRowsRequest.build())
        }
        .flatMap(_.asScala)
    }
  }

  /** Write rows to the specified table, family, column
    */
  def write(project: String,
            instanceId: String,
            table: String,
            family: String,
            column: String,
            batchSize: Int,
            rows: RDD[(String, Array[Byte])]): RDD[MutateRowsResponse] = {
    rows.mapPartitions{rows =>
      new BigTable.Writer(project, instanceId, table, family, column, batchSize)
        .write(rows)
    }
  }
}
