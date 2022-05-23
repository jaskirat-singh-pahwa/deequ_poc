import sys
from pyspark.sql import SparkSession

from src.args import parse_args
from src.spark_init import get_spark
from src.process_data import convert_excel_sheets_to_csv_files
from src.generic.rules import GenericRules


def main(argv) -> None:
    args = parse_args(argv)
    print(args["claims_data_path"])

    spark: SparkSession = get_spark()
    df = convert_excel_sheets_to_csv_files(spark=spark, excel_file_path=args["claims_data_path"], sheets=["Rx Fact"])

    generic_rules = GenericRules(spark=spark, df=df)
    print(generic_rules.is_complete(column_name="WEEK_END_FRI"))

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
