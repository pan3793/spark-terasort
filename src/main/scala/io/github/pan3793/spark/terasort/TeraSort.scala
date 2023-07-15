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

import java.util.Comparator
import com.google.common.primitives.UnsignedBytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a great example program to stress test Spark's shuffle mechanism.
 *
 * See http://sortbenchmark.org/
 */
object TeraSort extends Logging {

  implicit val caseInsensitiveOrdering: Comparator[Array[Byte]] =
    UnsignedBytes.lexicographicalComparator

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      logError(
        """
          |Usage:
          |  spark-submit \
          |    --conf xyz=abc \
          |    --class io.github.pan3793.spark.terasort.TeraSort \
          |    spark-terasort-<version>.jar \
          |    [input-path] [output-path]
          |
          |Example:
          |  spark-submit \
          |    --conf spark.executor.cores=4 \
          |    --conf spark.executor.memory=8g \
          |    --class io.github.pan3793.spark.terasort.TeraSort \
          |    spark-terasort-2.0.0.jar \
          |    hdfs:///benchmark/terasort_in_100g hdfs:///benchmark/terasort_out_100g
          |""".stripMargin)
      sys.exit(1)
    }

    val (inputPath, outputPathOpt) = args match {
      case Array(input, output) => input -> Some(output)
      case Array(input) => input -> None
      case _ => throw new IllegalArgumentException(args.mkString(" "))
    }

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setIfMissing("spark.app.name", "TeraSort")
    val sc = SparkContext.getOrCreate(conf)

    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputPath)
    val sorted =
      dataset.repartitionAndSortWithinPartitions(new TeraSortPartitioner(dataset.partitions.length))

    outputPathOpt match {
      case Some(outputPath) => sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputPath)
      case None => sorted.foreach { _ => (): Unit }
    }
  }
}
