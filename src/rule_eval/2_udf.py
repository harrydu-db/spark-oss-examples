from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType
from data_generator import generate_sample_data, get_rules_data
import time

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("UDF Expression Example") \
    .getOrCreate()

# Get records and rules data
records = generate_sample_data(num_rows=100)
records_df = spark.createDataFrame(records, ["id", "col1", "col2", "col3", "col4"])

rules_data = get_rules_data()
rules_df = spark.createDataFrame(rules_data, ["rule_id", "sql_exp"])

# Cross join records with rules to get all combinations
combined_df = records_df.crossJoin(rules_df)

# Define a UDF to evaluate the expression
def evaluate_expr(expression, col1, col2, col3, col4):
    try:
        expr = expression.replace("col1", str(col1)) \
                        .replace("col2", str(col2)) \
                        .replace("col3", str(col3)) \
                        .replace("col4", str(col4))
        return eval(expr)
    except Exception as e:
        return False

evaluate_expr_udf = udf(evaluate_expr, BooleanType())

start_time = time.time()

# Apply the rule evaluation using expr
result_df = combined_df.withColumn(
    "rule_passed",
    evaluate_expr_udf(col("sql_exp"), col("col1"), col("col2"), col("col3"), col("col4"))
)

# Show the results
result_df.show(truncate=False)

end_time = time.time()

print(f"Number of rows: {result_df.count()}")
print(f"Time taken: {end_time - start_time} seconds")

# Stop the SparkSession
spark.stop()
