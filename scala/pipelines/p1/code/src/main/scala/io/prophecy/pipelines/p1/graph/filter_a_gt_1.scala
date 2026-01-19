package io.prophecy.pipelines.p1.graph

import io.prophecy.libs._
import io.prophecy.pipelines.p1.functions.PipelineInitCode._
import io.prophecy.pipelines.p1.functions.UDFs._
import io.prophecy.pipelines.p1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object filter_a_gt_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(col("a") > lit(7))

}
