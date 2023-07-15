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

/**
 * This file is copied from Hadoop package org.apache.hadoop.examples.terasort.
 */

import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 * An output format that writes the key and value appended together.
 */
class TeraOutputFormat extends FileOutputFormat[Array[Byte], Array[Byte]] {

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[Array[Byte], Array[Byte]] = {
    val file: Path = getDefaultWorkFile(job, "")
    val fs: FileSystem = file.getFileSystem(job.getConfiguration)
    val fileOut: FSDataOutputStream = fs.create(file)
    new TeraRecordWriter(fileOut, job)
  }

  class TeraRecordWriter(val out: FSDataOutputStream, val job: JobContext)
    extends RecordWriter[Array[Byte], Array[Byte]] {

    override def write(key: Array[Byte], value: Array[Byte]): Unit = {
      out.write(key)
      out.write(value)
    }

    override def close(context: TaskAttemptContext): Unit = {
      out.close()
    }
  }
}
