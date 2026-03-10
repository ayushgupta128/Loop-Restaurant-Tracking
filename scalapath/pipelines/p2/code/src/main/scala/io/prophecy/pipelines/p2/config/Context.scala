package io.prophecy.pipelines.p2.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
