from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from p1.config.ConfigStore import *
from p1.functions import *

def seed1(spark: SparkSession) -> DataFrame:
    schemaFields = StructType([
        StructField("col1", StringType(), True), StructField("col2", StringType(), True), StructField("col3", StringType(), True)
    ])\
        .fields
    readSchema = StructType([StructField(f.name, StringType(), True) for f in schemaFields])

    return spark.createDataFrame([Row("1", "2", "3"), Row("11", "22", "33")], readSchema)
