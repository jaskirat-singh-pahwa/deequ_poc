import yaml
import boto3

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


class DataLoader:
    def __init__(
            self, spark: SparkSession,
            config_bucket: str,
            config_key: str
    ):
        self.spark = spark
        self.config_bucket = config_bucket
        self.config_key = config_key
        self.s3_client = boto3.client("s3")

    def load_constraints(self):
        s3_config_object = self.s3_client.get_object(
            Bucket=self.config_bucket,
            Key=self.config_key
        )
        constraints = yaml.load(
            s3_config_object["Body"].read().decode(),
            Loader=yaml.FullLoader
        )
        return constraints

    def get_latest_s3_data_path(self, s3_data_bucket: str, s3_data_prefix: str) -> str:
        response = self.s3_client.list_objects_v2(
            Bucket=s3_data_bucket,
            Prefix=s3_data_prefix,
            Delimiter="/"
        )
        for prefix in response["CommonPrefixes"]:
            pass

    def load_csv(self):
        pass

    def load_parquet(self):
        pass

    def load_from_redshift(self):
        pass

    def get_dataframe(self) -> DataFrame:
        constraints = self.load_constraints()
        s3_data_bucket = constraints["dq_config"]["dataset"]["s3_bucket"]
        s3_data_prefix = constraints["dq_config"]["dataset"]["s3_key"]

        latest_s3_data_path: str = self.get_latest_s3_data_path(
            s3_data_bucket=s3_data_bucket,
            s3_data_prefix=s3_data_prefix
        )

        return self.spark \
            .read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(latest_s3_data_path)


