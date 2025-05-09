from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, struct
from pyspark.sql.types import BooleanType, StructType, StructField, StringType
import pandas as pd

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Pandas UDF Expression Example") \
    .getOrCreate()

# Define column names
RECORD_COLUMNS = ["id", "col1", "col2"]

# Sample data
records = [
    (1, 10, 5),
    (2, 20, 15),
    (3, 30, 25)
]
records_df = spark.createDataFrame(records, RECORD_COLUMNS)

rules_data = [
    (1, "col1 > col2"),
    (2, "col1 < col2"),
    (3, "col1 == col2")
]
rules_df = spark.createDataFrame(rules_data, ["rule_id", "sql_exp"])

# Cross join records with rules to get all combinations
combined_df = records_df.crossJoin(rules_df)

# Define a Pandas UDF to evaluate the expression
@pandas_udf(BooleanType())
def evaluate_expr(expression: pd.Series, column_values: pd.DataFrame) -> pd.Series:
    results = []
    for expr, values in zip(expression, column_values.itertuples(index=False)):
        try:
            # Replace column names with their values
            eval_expr = expr
            for col_name, value in zip(RECORD_COLUMNS, values):
                if col_name in eval_expr:
                    eval_expr = eval_expr.replace(col_name, str(value))
            results.append(eval(eval_expr))
        except Exception as e:
            results.append(False)
    return pd.Series(results)

# Apply the rule evaluation using pandas_udf
result_df = combined_df.withColumn(
    "rule_passed",
    evaluate_expr(
        col("sql_exp"),
        struct([col(c) for c in RECORD_COLUMNS])
    )
)

# Show the results
result_df.show(truncate=False)

# Stop the SparkSession
spark.stop()
