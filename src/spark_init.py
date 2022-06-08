import pydeequ
import pyspark
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    conf = pyspark.SparkConf().setAll([
        ("spark.jars.packages", 'com.amazon.deequ:deequ:1.1.0_spark-3.0-scala-2.12'),
        ("spark.jars.excludes", pydeequ.f2j_maven_coord),
        ("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.14.0")

    ])

    spark = (
        SparkSession
        .builder
        .master("local")
        .config(conf=conf)
        .appName("iqvia_deequ_framework_v2")
        .getOrCreate()
    )

    return spark




