from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from p1.config.ConfigStore import *
from p1.functions import *

def bbh(spark: SparkSession) -> DataFrame:
    df = spark.read\
             .format("jdbc")\
             .option("url", f"{Config.super2}")\
             .option("user", f"{Config.super2}")\
             .option("password", f"{Config.sub1}")\
             .option("query", "select c1, c2 from t1")\
             .option("pushDownPredicate", True)\
             .option("driver", "org.postgresql.Driver")\
             .load()

    return df
