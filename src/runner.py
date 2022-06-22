"""
Create a public class where you will load all the variables
in constraints and share that class with DataLoader class
to load all the paths
"""

from pyspark.sql import SparkSession

from src.data_loader import DataLoader
from src.deequ.suggestions import get_suggestions


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

    def deequ_runner(self):
        pass

    def great_expectations_runner(self):
        pass

    def run_data_quality(self):
        load_data = DataLoader(
            spark=self.spark,
            config_bucket=self.config_bucket,
            config_key=self.config_key
        )

        config = load_data.load_constraints()

        if config["data_quality_framework"] == "deequ".lower():
            pass


import yaml
import boto3


def read_yaml():
    s3_client = boto3.client("s3",
                               aws_access_key_id="AKIAXPQIZQGDUHSBFZ7A",
                               aws_secret_access_key="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"
                               )

    with open("/Users/jaskirat/Tiger/iqvia_deequ/constraints/iris.yaml", "r") as stream:
        try:
            constraints = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    print(constraints)

    s3_bucket = constraints["dq_config"]["dataset"]["s3_bucket"]
    s3_key = constraints["dq_config"]["dataset"]["s3_key"]
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key, Delimiter="/")
    years = []
    months = []
    days = []

    for prefix in response["CommonPrefixes"]:
        print(int(prefix["Prefix"].split("year=")[1].split("/")[0]))

    # latest_year = ""
    # latest_month = ""
    # latest_day = ""
    # latest_data = ""
    #
    # s3_path = f"s3://{s3_bucket}/{s3_key}/{latest_data}/"
    # print(s3_path)


if __name__ == "__main__":
    read_yaml()
