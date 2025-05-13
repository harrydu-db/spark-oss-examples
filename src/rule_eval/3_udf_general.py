from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct
from pyspark.sql.types import BooleanType, StructType, StructField, StringType
from data_generator import generate_sample_data, get_rules_data
import time

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("UDF Expression Example") \
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

# Define a UDF to evaluate the expression
def evaluate_expr(expression, column_values):
    try:
        # Replace column names with their values
        expr = expression
        for col_name in RECORD_COLUMNS:
            if col_name in expr:
                expr = expr.replace(col_name, str(column_values[col_name]))
        return eval(expr)
    except Exception as e:
        return False

# Create struct type for column values
struct_type = StructType([
    StructField(col_name, StringType(), True) for col_name in RECORD_COLUMNS
])

evaluate_expr_udf = udf(evaluate_expr, BooleanType())

start_time = time.time()

# Apply the rule evaluation using expr
result_df = combined_df.withColumn(
    "rule_passed",
    evaluate_expr_udf(
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
