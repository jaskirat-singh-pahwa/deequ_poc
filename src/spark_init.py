import pydeequ
import pyspark
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    conf = pyspark.SparkConf().setAll([
        ("spark.jars.packages", "com.amazon.deequ:deequ:1.1.0_spark-3.0-scala-2.12"),
        ("spark.jars.excludes", pydeequ.f2j_maven_coord)
    ])

    spark = (
        SparkSession
        .builder
        .config(conf=conf)
        .appName("iqvia_deequ_framework_v1")
        .getOrCreate()
    )

    return spark
