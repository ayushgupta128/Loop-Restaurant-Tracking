from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Customer_Orders(spark: SparkSession, in0: DataFrame):
    if Config.fabricName == "dev":
        in0.write\
            .option("header", True)\
            .option("sep", ",")\
            .option("ignoreLeadingWhiteSpace", True)\
            .option("ignoreTrailingWhiteSpace", True)\
            .mode("overwrite")\
            .option("separator", ",")\
            .option("header", True)\
            .csv("dbfs:/Prophecy/a561c4f9c3cc2b29f13c891b1dc1b5d3/CustomersOrders.csv")
    else:
        raise Exception("No valid dataset present to read fabric")
