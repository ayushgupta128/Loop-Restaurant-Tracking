package io.prophecy.pipelines.p1.graph

import io.prophecy.libs._
import io.prophecy.pipelines.p1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object ds {

  def apply(context: Context): DataFrame = {
    val spark = context.spark
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        List(Row.fromSeq(List("9", "2", "3")),
             Row.fromSeq(List("2", "2", "3")),
             Row.fromSeq(List("7", "2", "3"))
        )
      ),
      StructType(
        StructType(
          Array(StructField("a", StringType, true),
                StructField("b", StringType, true),
                StructField("c", StringType, true)
          )
        ).fields.map(fieldName => StructField(fieldName.name, StringType, true))
      )
    )
  }

}
