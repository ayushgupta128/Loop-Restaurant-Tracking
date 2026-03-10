package io.prophecy.pipelines.p2.graph

import io.prophecy.libs._
import io.prophecy.pipelines.p2.functions.PipelineInitCode._
import io.prophecy.pipelines.p2.functions.UDFs._
import io.prophecy.pipelines.p2.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object select_abc {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("a"), col("b"), col("c"))

}
