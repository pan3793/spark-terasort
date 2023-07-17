TeraSort benchmark for Spark
===
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.pan3793/spark-terasort_2.12/badge.svg)](https://central.sonatype.com/search?name=spark-terasort&namespace=io.github.pan3793)
[![License](https://img.shields.io/github/license/pan3793/spark-terasort)](https://github.com/pan3793/spark-terasort/blob/master/LICENSE)

This is a Spark application for generating TeraSort data and running TeraSort benchmarks.
It is variant of TeraSort program listed on http://sortbenchmark.org/.

This is fork and rework of [ehiggs/spark-terasort](https://github.com/ehiggs/spark-terasort),

# Building

```shell
build/mvn clean package -Pscala-2.12 -Dspark.version=3.3.2
```

# Running

## Generate data

```shell
$SPARK_HOME/bin/spark-submit \
  --class io.github.pan3793.spark.terasort.TeraGen \
  spark-terasort_2.12-2.0.0.jar \
  <size> <generate_data_path>
```
or
```shell
$SPARK_HOME/bin/spark-submit \
  --class io.github.pan3793.spark.terasort.TeraGen \
  --packages io.github.pan3793:spark-terasort_2.12:2.0.0 \
  spark-internal \
  <size> <generate_data_path>
```

## Sort the data

```shell
$SPARK_HOME/bin/spark-submit \
  --class io.github.pan3793.spark.terasort.TeraSort \
  spark-terasort_2.12-2.0.0.jar \
  <input_data_path> <output_data_path>
```
or
```shell
$SPARK_HOME/bin/spark-submit \
  --class io.github.pan3793.spark.terasort.TeraSort \
  --packages io.github.pan3793:spark-terasort_2.12:2.0.0 \
  spark-internal \
  <input_data_path> <output_data_path>
```

## Validate the data

```shell
$SPARK_HOME/bin/spark-submit \
  --class io.github.pan3793.spark.terasort.TeraValidate \
  spark-terasort_2.12-2.0.0.jar \
  <output_data_path>
```
or
```shell
$SPARK_HOME/bin/spark-submit \
  --class io.github.pan3793.spark.terasort.TeraValidate \
  --packages io.github.pan3793:spark-terasort_2.12:2.0.0 \
  spark-internal \
  <output_data_path>
```

# Contributing

PRs are very welcome!
