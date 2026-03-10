package io.prophecy.pipelines.p1

import io.prophecy.libs._
import io.prophecy.pipelines.p1.config._
import io.prophecy.pipelines.p1.functions.UDFs._
import io.prophecy.pipelines.p1.functions.PipelineInitCode._
import io.prophecy.pipelines.p1.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_seed1    = seed1(context)
    val df_Filter_1 = Filter_1(context, df_seed1)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession =
      SparkSession.builder().appName("p1").enableHiveSupport().getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",        "pipelines/p1")
    spark.conf.set("spark.default.parallelism",             "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/p1") {
      apply(context)
    }
  }

}
