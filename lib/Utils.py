import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config

env=os.getenv('LOCAL')

def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .master("local[2]") \
            .getOrCreate()
    else:
        return SparkSession.builder \
        .config(conf=get_pyspark_config(env)) \
        .enableHiveSupport() \
        .getOrCreate()