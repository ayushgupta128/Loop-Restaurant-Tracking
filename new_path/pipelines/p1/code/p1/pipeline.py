from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from p1.config.ConfigStore import *
from p1.functions import *
from prophecy.utils import *
from p1.graph import *

def pipeline(spark: SparkSession) -> None:
    df_seed1_1 = seed1_1(spark)
    df_Filter_1 = Filter_1(spark, df_seed1_1)
    df_limit_100_rows = limit_100_rows(spark, df_Filter_1)
    tgt(spark, df_limit_100_rows)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("p1").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/p1")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/p1", config = Config)(pipeline)

if __name__ == "__main__":
    main()
