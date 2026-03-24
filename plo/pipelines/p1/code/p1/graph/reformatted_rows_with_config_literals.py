from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from p1.config.ConfigStore import *
from p1.functions import *

def reformatted_rows_with_config_literals(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("nib"))
