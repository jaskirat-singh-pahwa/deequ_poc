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

    def get_max_date_parameter(
            self,
            s3_data_bucket: str,
            s3_data_prefix: str,
            date_parameter: str,
            delimiter: str
    ) -> int:
        parameters = []
        response = self.s3_client.list_objects_v2(Bucket=s3_data_bucket, Prefix=s3_data_prefix, Delimiter=delimiter)
        for prefix in response["CommonPrefixes"]:
            parameters.append(int(prefix["Prefix"].split(date_parameter)[1].split(delimiter)[0]))

        return max(parameters)

    @staticmethod
    def get_latest_s3_data_path(
            s3_data_bucket: str,
            s3_data_prefix: str,
            year: str,
            month: str,
            day: str
    ) -> str:
        return f"s3://{s3_data_bucket}/{s3_data_prefix}year={year}/month={month}/day={day}"

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

        date_parameters = {
            "year": None,
            "month": None,
            "day": None
        }

        for date_parameter in date_parameters.keys():
            if date_parameter == "year":
                new_s3_data_prefix = s3_data_prefix

            elif date_parameter == "month":
                new_s3_data_prefix = f"{s3_data_prefix}year={date_parameters['year']}/"

            else:
                new_s3_data_prefix = f"{s3_data_prefix}year={date_parameters['year']}/" \
                                     f"month={date_parameters['month']}/"

            date_parameters[date_parameter] = self.get_max_date_parameter(
                s3_data_bucket=s3_data_bucket,
                s3_data_prefix=new_s3_data_prefix,
                date_parameter=f"{date_parameter}=",
                delimiter="/"
            )

        latest_s3_data_path: str = self.get_latest_s3_data_path(
            s3_data_bucket=s3_data_bucket,
            s3_data_prefix=s3_data_prefix,
            year=date_parameters["year"],
            month=date_parameters["month"],
            day=date_parameters["day"]
        )

        return self.spark \
            .read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(latest_s3_data_path)
