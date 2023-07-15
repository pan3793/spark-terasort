/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pan3793.spark.terasort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.util.PureJavaCrc32
import com.google.common.primitives.UnsignedBytes

/**
 * An application that reads sorted data according to the terasort spec and
 * reports if it's indeed sorted.
 * This is an example program to validate TeraSort results
 *
 * See http://sortbenchmark.org/
 */
object TeraValidate extends Logging {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      logError(
        """
          |Usage:
          |  spark-submit \
          |    --conf xyz=abc \
          |    --class io.github.pan3793.spark.terasort.TeraValidate \
          |    spark-terasort-<version>.jar \
          |    [input-path]
          |
          |Example:
          |  spark-submit \
          |    --conf spark.executor.cores=4 \
          |    --conf spark.executor.memory=8g \
          |    --class io.github.pan3793.spark.terasort.TeraValidate \
          |    spark-terasort-2.0.0.jar \
          |    hdfs:///benchmark/terasort_out_100g
          |""".stripMargin)
      sys.exit(-1)
    }

    // Process command line arguments
    val inputFile = args(0)

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setIfMissing("spark.app.name", "TeraValidate")
    val sc = SparkContext.getOrCreate(conf)

    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile)
    validate(dataset)
  }

  def validate(dataset: RDD[(Array[Byte], Array[Byte])]): Unit = {
    val output: RDD[(Unsigned16, Array[Byte], Array[Byte])] =
      dataset.mapPartitions(
        iter => {
          val sum = new Unsigned16
          val checksum = new Unsigned16
          val crc32 = new PureJavaCrc32()
          val min = new Array[Byte](10)
          val max = new Array[Byte](10)

          val cmp = UnsignedBytes.lexicographicalComparator()

          var pos = 0L
          var prev = new Array[Byte](10)

          while (iter.hasNext) {
            val key = iter.next()._1
            assert(cmp.compare(key, prev) >= 0)

            crc32.reset()
            crc32.update(key, 0, key.length)
            checksum.set(crc32.getValue)
            sum.add(checksum)

            if (pos == 0) {
              key.copyToArray(min, 0, 10)
            }
            pos += 1
            prev = key
          }
          prev.copyToArray(max, 0, 10)
          Iterator((sum, min, max))
        },
        preservesPartitioning = true)

    val checksumOutput = output.collect()
    val cmp = UnsignedBytes.lexicographicalComparator()
    val numRecords = dataset.count
    val sum = new Unsigned16

    checksumOutput.foreach { case (partSum, _, _) =>
      sum.add(partSum)
    }
    var lastMax = new Array[Byte](10)
    checksumOutput.map { case (partSum, min, max) =>
      (partSum, min.clone(), max.clone())
    }.zipWithIndex.foreach { case ((_, min, max), i) =>
      logInfo(s"part $i")
      logInfo(s"lastMax" + lastMax.toSeq.map(x => if (x < 0) 256 + x else x))
      logInfo(s"min " + min.toSeq.map(x => if (x < 0) 256 + x else x))
      logInfo(s"max " + max.toSeq.map(x => if (x < 0) 256 + x else x))
      require(cmp.compare(min, max) <= 0, "min >= max")
      require(cmp.compare(lastMax, min) <= 0, "current partition min < last partition max")
      lastMax = max
    }
    logInfo(s"num records: $numRecords")
    logInfo(s"checksum: $sum")
    logInfo("partitions are properly sorted")
  }
}
