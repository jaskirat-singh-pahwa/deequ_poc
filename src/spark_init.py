import pydeequ
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    return (
        SparkSession
        .builder
        .master("local")
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.14.0")
        .appName("iqvia_deequ_framework")
        .getOrCreate()
    )
