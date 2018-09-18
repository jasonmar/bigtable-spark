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

import java.nio.ByteBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/** Spark Job which writes to BigTable */
object BigTableExample {
  val AppName = "BigTable Spark Example"
  val BatchSize = 42000

  /** Example class which can be reduced and marshalled to byte array */
  case class Reduceable(data: Array[Long]) {
    def + (other: Reduceable): Reduceable = {
      var i = 0
      while (i < data.length) {
        data(i) += other.data(i)
        i += 1
      }
      this
    }

    // TODO implement your own serialization
    def toBytes: Array[Byte] = {
      val buf = ByteBuffer.allocate(data.length*8)
      val lb = buf.asLongBuffer()
      var i = 0
      while (i < data.length) {
        lb.put(data(i))
        i += 1
      }
      buf.array()
    }

    def toWriteable(family: String, column: String, rowKey: String): Seq[(String, BigTable.Writeable)] = {
      val writeable = BigTable.Writeable(toBytes, family, column)
      // TODO come up with your own logic for deciding when multiple row keys are needed
      if (rowKey.startsWith("sorted#")) { // example use of prefix to determine key elements
        Seq(
          (rowKey + s"#amt#${data(0)}", writeable), // sorted by value 0
          (rowKey + s"#qty#${data(1)}", writeable)  // sorted by value 1
        )
      } else {
        Seq(
          (rowKey, writeable)
        )
      }
    }
  }

  def main(args: Array[String]){
    val project = args(0)
    val instanceId = args(1)
    val table = args(2)
    val family = args(3)
    val column = args(4)

    if (args.length != 5) {
      System.err.println("Invalid arguments")
      System.exit(1)
    }

    val spark = SparkSession.builder.appName(AppName).getOrCreate()
    import spark.implicits._

    val data: RDD[(String, Reduceable)] = // TODO load data from somewhere
      Seq.empty[(String,Reduceable)].toDS().rdd

    val rdd: RDD[(String,BigTable.Writeable)] =
      data
        .reduceByKey(_ + _)
        .flatMap{tuple =>
          val (rowKey, data) = tuple
          data.toWriteable(family, column, rowKey)
        }

    BigTable.write(project, instanceId, table, BatchSize, rdd)

    spark.stop()
  }
}
