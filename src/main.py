import sys

from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from awsglue.context import GlueContext
from awsglue.job import Job

from src.args import parse_args
from src.spark_init import get_spark
from src.runner import Runner


def main(argv) -> None:
    args = parse_args(input_args=argv)

    job_name: str = args["JOB_NAME"]
    config_file_bucket: str = args["CONFIG_FILE_BUCKET"]
    config_file_key: str = args["CONFIG_FILE_KEY"]

    spark_context = SparkContext()

    glue_context = GlueContext(spark_context)

    job = Job(glue_context)
    job.init(job_name, args)

    spark: SparkSession = get_spark()

    runner = Runner(
        spark=spark,
        config_file_bucket=config_file_bucket,
        config_file_key=config_file_key
    )
    runner.run_data_quality()

    job.commit()


if __name__ == "__main__":
    main(sys.argv)
