from typing import List
from pyspark.sql import SparkSession


def convert_excel_sheets_to_csv_files(spark, excel_file_path: str, sheets: List[str]):
    for sheet in sheets:
        df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("dataAddress", f"'{sheet}'!") \
            .load(excel_file_path)

        print(df.printSchema())
        print(df.count())

        df.coalesce(1) \
            .write \
            .option("header", "true") \
            .csv(f"/Users/jaskirat/Tiger/iqvia_deequ/data/processed/{sheet}.csv")


    # df = spark.read.option("header", "false").option("inferSchema", "true").csv(excel_file_path)

        # return df
