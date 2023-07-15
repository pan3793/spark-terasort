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

import scala.collection.JavaConverters._

import java.util.Comparator
import java.util.{List => JList}

import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit

object TeraInputFormat {
  val KEY_LEN = 10
  val VALUE_LEN = 90
  val RECORD_LEN: Int = KEY_LEN + VALUE_LEN
  implicit val caseInsensitiveOrdering: Comparator[Array[Byte]] =
    UnsignedBytes.lexicographicalComparator
}

class TeraInputFormat extends FileInputFormat[Array[Byte], Array[Byte]] {

  // Sort the file pieces since order matters.
  override def listStatus(job: JobContext): JList[FileStatus] = {
    // HADOOP-16196 change the method signature
    super.listStatus(job).asScala.sortBy { fs => fs.getPath.toUri }.asJava
  }

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[Array[Byte], Array[Byte]] = new TeraRecordReader()

  class TeraRecordReader extends RecordReader[Array[Byte], Array[Byte]] {
    private var in: FSDataInputStream = _
    private var offset: Long = 0
    private var length: Long = 0
    private val key: Array[Byte] = new Array[Byte](TeraInputFormat.KEY_LEN)
    private val value: Array[Byte] = new Array[Byte](TeraInputFormat.VALUE_LEN)

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      val fileSplit = split.asInstanceOf[FileSplit]
      val p: Path = fileSplit.getPath
      val fs: FileSystem = p.getFileSystem(context.getConfiguration)
      in = fs.open(p)
      val start: Long = fileSplit.getStart
      // find the offset to start at a record boundary
      val reclen = TeraInputFormat.RECORD_LEN
      offset = (reclen - (start % reclen)) % reclen
      in.seek(start + offset)
      length = fileSplit.getLength
    }

    override def nextKeyValue(): Boolean = {
      if (offset >= length) {
        return false
      }
      in.readFully(key)
      in.readFully(value)
      offset += TeraInputFormat.RECORD_LEN
      true
    }

    override def getCurrentKey: Array[Byte] = key

    override def getCurrentValue: Array[Byte] = value

    override def getProgress: Float = offset * 1.0f / length

    override def close(): Unit = in.close()
  }

}
