from typing import List
from pyspark.sql import SparkSession


def convert_excel_sheets_to_csv_files(spark: SparkSession, excel_file_path: str, sheets: List[str]):
    # for sheet in sheets:
    df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("dataAddress", "'Rx Fact'!") \
        .load(excel_file_path)

    print(df.printSchema())
    print(df.count())

    return df

