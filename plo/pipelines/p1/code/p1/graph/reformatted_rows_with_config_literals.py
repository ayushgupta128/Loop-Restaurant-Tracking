from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from p1.config.ConfigStore import *
from p1.functions import *

def reformatted_rows_with_config_literals(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(Config.super1).alias("sup1"), 
        lit(Config.super2).alias("sup2"), 
        lit(Config.sub1).alias("sub1"), 
        lit(Config.sub2).alias("sub2"), 
        col("col1"), 
        col("col2")
    )
