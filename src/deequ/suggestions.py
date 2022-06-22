from pydeequ.suggestions import *

from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext


def get_suggestions(spark: SparkSession, df: DataFrame) -> DataFrame:
    spark_context = SparkContext()
    raw_suggestions = ConstraintSuggestionRunner(spark) \
        .onData(df) \
        .addConstraintRule(CompleteIfCompleteRule()) \
        .addConstraintRule(NonNegativeNumbersRule()) \
        .addConstraintRule(RetainCompletenessRule()) \
        .addConstraintRule(RetainTypeRule()) \
        .addConstraintRule(UniqueIfApproximatelyUniqueRule()) \
        .run()

    json_suggestions = json.dumps(raw_suggestions["constraint_suggestions"])
    suggestions: DataFrame = spark \
        .read \
        .json(spark_context.parallelize([json_suggestions]))

    return suggestions
