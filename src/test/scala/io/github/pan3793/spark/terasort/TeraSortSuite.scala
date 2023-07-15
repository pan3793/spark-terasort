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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path, Paths}

class TeraSortSuite extends AnyFunSuite with BeforeAndAfterAll with Logging {

  val DATA_SIZE: String = sys.env.getOrElse("TERA_SORT_SIZE", "256m")

  val inputPath: Path =
    Paths.get(sys.props("java.io.tmpdir"), s"terasort_in_$DATA_SIZE").toAbsolutePath
  val outputPath: Path =
    Paths.get(sys.props("java.io.tmpdir"), s"terasort_out_$DATA_SIZE").toAbsolutePath

  override def beforeAll(): Unit = {
    Files.deleteIfExists(inputPath)
    Files.deleteIfExists(outputPath)
  }

  test(s"Tera Gen/Sort/Validate - $DATA_SIZE") {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setIfMissing("spark.app.name", s"Tera Gen/Sort/Validate - $DATA_SIZE")
    SparkContext.getOrCreate(conf)

    TeraGen.main(Array(DATA_SIZE, inputPath.toString))
    TeraSort.main(Array(inputPath.toString, outputPath.toString))
    TeraValidate.main(Array(outputPath.toString))
  }
}
