from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from p1.config.ConfigStore import *
from p1.functions import *

def vfv(spark: SparkSession) -> DataFrame:
    schemaFields = StructType([StructField("nib", StringType(), True)]).fields
    readSchema = StructType([StructField(f.name, StringType(), True) for f in schemaFields])

    return spark.createDataFrame([Row("nhjkh")], readSchema)
