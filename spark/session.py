from pyspark.sql import SparkSession
from config.settings import SPARK_APP_NAME, SPARK_MASTER

def get_spark():
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
