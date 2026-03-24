from pyspark.sql import SparkSession
from prophecy.config.utils import *
from .Config import Config as ConfigClass
Config: ConfigClass = ConfigClass()


class Utils:
    @staticmethod
    def initializeFromArgs(spark: SparkSession, args):
        global Config
        Config.updateSpark(spark)
        conf = parse_config(args, config_package = "prophecy_config_instances.oracletest")
        prophecy_project_config = parse_project_config(args)
        Config.update(**{**{"prophecy_project_config" : prophecy_project_config}, **conf})
