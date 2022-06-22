from typing import Dict

from pyspark.sql import DataFrame

"""
Create a class for this:
In the init, take the config for writing data, whether a redshift config or s3, etc as dict
Write different functions inside the class and create a driver function in the class

"""


class DataWriter:
    def __init__(self, df: DataFrame, config: Dict[str, str]):
        self.df = df
        self.config = config

    def write_dataframe_to_s3(self):
        s3_path = self.config["s3_write_path"]

        self.df \
            .write \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(path=s3_path)

    def write_dataframe_to_redshift(self):
        pass

    def write_data(self):
        if len(self.config) != 0 and self.config["destination"].lower() == "s3":
            self.write_dataframe_to_s3()
        else:
            print("No configuration for writing data")
