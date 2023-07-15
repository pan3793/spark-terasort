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

object TeraGen extends Logging {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      logError(
        """
          |Usage:
          |  spark-submit \
          |    --conf xyz=abc \
          |    --class io.github.pan3793.spark.terasort.TeraGen \
          |    spark-terasort-<version>.jar \
          |    [output-size] [output-directory]
          |
          |Example:
          |  spark-submit \
          |    --conf spark.executor.cores=4 \
          |    --conf spark.executor.memory=8g \
          |    --class io.github.pan3793.spark.terasort.TeraGen \
          |    spark-terasort_2.12-2.0.0.jar \
          |    100g hdfs:///benchmark/terasort_in_100g
          |""".stripMargin)
      sys.exit(1)
    }

    // Process command line arguments
    val outputSizeInBytes = Utils.sizeStrToBytes(args(0))
    val outputFile = args(1)

    val size = Utils.sizeToSizeStr(outputSizeInBytes)

    val conf = new SparkConf()
      .setIfMissing("spark.app.name", s"TeraGen ($size)")

    val sc = SparkContext.getOrCreate(conf)

    val sizeInBytesPerPartition = sc.getConf.getSizeAsBytes("spark.files.maxPartitionBytes", "128m")
    val recordsPerPartition = sizeInBytesPerPartition / TeraInputFormat.RECORD_LEN
    val parts = (outputSizeInBytes / sizeInBytesPerPartition).toInt
    val numRecords = recordsPerPartition * parts

    logInfo(
      s"""
         |===========================================================================
         |===========================================================================
         |Input size: $size
         |Total number of records: $numRecords
         |Number of output partitions: $parts
         |Number of records/output partition: ${numRecords / parts}
         |===========================================================================
         |===========================================================================
         |""".stripMargin)

    assert(recordsPerPartition < Int.MaxValue, s"records per partition > ${Int.MaxValue}")

    val dataset = sc.parallelize(1 to parts, parts).mapPartitionsWithIndex { case (index, _) =>
      val one = new Unsigned16(1)
      val firstRecordNumber = new Unsigned16(index.toLong * recordsPerPartition)
      val recordsToGenerate = new Unsigned16(recordsPerPartition)

      val recordNumber = new Unsigned16(firstRecordNumber)
      val lastRecordNumber = new Unsigned16(firstRecordNumber)
      lastRecordNumber.add(recordsToGenerate)

      val rand = Random16.skipAhead(firstRecordNumber)

      val key = new Array[Byte](TeraInputFormat.KEY_LEN)
      val value = new Array[Byte](TeraInputFormat.VALUE_LEN)

      Iterator.tabulate(recordsPerPartition.toInt) { _ =>
        Random16.nextRand(rand)
        generateRecord(key, value, rand, recordNumber)
        recordNumber.add(one)
        (key, value)
      }
    }

    dataset.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)

    logInfo(s"Number of records written: ${dataset.count()}")
  }

  /**
   * Generate a binary record suitable for all sort benchmarks except PennySort.
   */
  def generateRecord(
      keyBuf: Array[Byte],
      valueBuf: Array[Byte],
      rand: Unsigned16,
      recordNumber: Unsigned16): Unit = {
    // Generate the 10-byte key using the high 10 bytes of the 128-bit random number
    var i = 0
    while (i < 10) {
      keyBuf(i) = rand.getByte(i)
      i += 1
    }

    // Add 2 bytes of "break"
    valueBuf(0) = 0x00.toByte
    valueBuf(1) = 0x11.toByte

    // Convert the 128-bit record number to 32 bits of ascii hexadecimal
    // as the next 32 bytes of the record.
    i = 0
    while (i < 32) {
      valueBuf(2 + i) = recordNumber.getHexDigit(i).toByte
      i += 1
    }

    // Add 4 bytes of "break" data
    valueBuf(34) = 0x88.toByte
    valueBuf(35) = 0x99.toByte
    valueBuf(36) = 0xAA.toByte
    valueBuf(37) = 0xBB.toByte

    // Add 48 bytes of filler based on low 48 bits of random number
    i = 0
    while (i < 12) {
      val v = rand.getHexDigit(20 + i).toByte
      valueBuf(38 + i * 4) = v
      valueBuf(39 + i * 4) = v
      valueBuf(40 + i * 4) = v
      valueBuf(41 + i * 4) = v
      i += 1
    }

    // Add 4 bytes of "break" data
    valueBuf(86) = 0xCC.toByte
    valueBuf(87) = 0xDD.toByte
    valueBuf(88) = 0xEE.toByte
    valueBuf(89) = 0xFF.toByte
  }
}
