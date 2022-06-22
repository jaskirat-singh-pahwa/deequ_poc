from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pydeequ.checks import *
from pydeequ.verification import *
#     VerificationSuite,
#     VerificationResult
# )
from pydeequ.analyzers import *


class GenericRules:
    def __init__(self, spark: SparkSession, df: DataFrame):
        self.spark = spark
        self.df = df
        self.check = Check(self.spark, CheckLevel.Error, "Quality Check")
        self.constraints: List[Check] = []
        self.verification_result = ""

    def check_constraints(self):
        self.check.addConstraints(self.constraints)
        verification_result = VerificationSuite(self.spark).onData(self.df).addCheck(self.check).run()
        print(type(verification_result))

        return VerificationResult.checkResultsAsDataFrame(self.spark,  verification_result)

    def is_complete(self, column_name: str) -> None:
        self.constraints.append(self.check.isComplete(column=column_name))
        pass

    def testing(self, column_name: str):
        analysisResult = AnalysisRunner(self.spark) \
            .onData(self.df) \
            .addAnalyzer(Size()) \
            .addAnalyzer(Completeness(column=column_name)) \
            .run()

        analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, analysisResult)
        print(analysisResult_df.show())


