package io.prophecy.pipelines.p1.graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.pipelines.p1.config._
import io.prophecy.libs.registerAllUDFs
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.math.BigDecimal

@RunWith(classOf[JUnitRunner])
class filter_a_gt_1Test extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._
  var context: Context = null

  test("Unit Test ") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/p1/graph/filter_a_gt_1/in/schema.json",
      "/data/io/prophecy/pipelines/p1/graph/filter_a_gt_1/in/data/unit_test_.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/p1/graph/filter_a_gt_1/out/schema.json",
      "/data/io/prophecy/pipelines/p1/graph/filter_a_gt_1/out/data/unit_test_.json",
      "out"
    )

    val dfOutComputed =
      io.prophecy.pipelines.p1.graph.filter_a_gt_1(context, dfIn)
    val res = assertDFEquals(dfOut.select("a", "b", "c"),
                             dfOutComputed.select("a", "b", "c"),
                             maxUnequalRowsToShow,
                             1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  override def beforeAll() = {
    super.beforeAll()
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerAllUDFs(spark)

    val fabricName = System.getProperty("fabric", "default")
    val confFilePath = Paths
      .get(
        getClass
          .getResource(s"/io/prophecy/pipelines/p1/${fabricName}.json")
          .toURI
      )
      .toString

    val config =
      ConfigurationFactoryImpl.getConfig(Array("--confFile", confFilePath))

    context = Context(spark, config)
  }

}
