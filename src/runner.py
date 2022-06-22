"""
Create a public class where you will load all the variables
in constraints and share that class with DataLoader class
to load all the paths
"""

from pyspark.sql import SparkSession, DataFrame

from src.data_loader import DataLoader
from src.deequ.suggestions import get_suggestions
from src.data_writer import DataWriter


class Runner:
    def __init__(
            self,
            spark: SparkSession,
            config_bucket: str,
            config_key: str
    ):
        self.spark = spark
        self.config_bucket = config_bucket
        self.config_key = config_key

        self.load_data = DataLoader(
            spark=self.spark,
            config_bucket=self.config_bucket,
            config_key=self.config_key
        )

        self.config = self.load_data.load_constraints()

    def deequ_runner(self):
        dataset: DataFrame = self.load_data.get_dataframe()
        suggestions: DataFrame = get_suggestions(
            spark=self.spark,
            df=dataset
        )

        s3_write_path = "s3://jaskirat-deequ-poc/data-quality-metrics/suggestions/year=2022/month=6/day=16/"
        write_config = {"destination": "s3", "s3_write_path": s3_write_path}

        data_writer = DataWriter(
            df=suggestions,
            config=write_config
        )

        data_writer.write_data()

    def great_expectations_runner(self):
        pass

    def run_data_quality(self):
        if self.config["data_quality_framework"] == "deequ".lower():
            self.deequ_runner()
