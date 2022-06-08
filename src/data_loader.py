import yaml
from typing import List
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


class DataLoader:
    def __init__(self, spark: SparkSession, constraints_path: str):
        self.spark = spark
        self.constraints_path = constraints_path

    def load_constraints(self):
        pass

    def load_csv(self):
        pass

    def load_parquet(self):
        pass

    def load_from_redshift(self):
        pass

    def get_dataframe(self):
        pass
