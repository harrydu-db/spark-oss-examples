from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, struct
from pyspark.sql.types import BooleanType, StructType, StructField, StringType
from data_generator import generate_sample_data, get_rules_data
import pandas as pd
import time

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Pandas UDF Expression Example") \
    .getOrCreate()

# Define column names
RECORD_COLUMNS = ["id", "col1", "col2", "col3", "col4"]

# Get records and rules data
records = generate_sample_data(num_rows=100)
records_df = spark.createDataFrame(records, RECORD_COLUMNS)

rules_data = get_rules_data()
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

start_time = time.time()

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

end_time = time.time()

print(f"Number of rows: {result_df.count()}")
print(f"Time taken: {end_time - start_time} seconds")

# Stop the SparkSession
spark.stop()
