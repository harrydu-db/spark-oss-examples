from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("UDF Expression Example") \
    .getOrCreate()

# Sample data
records = [
    (1, 10, 5),
    (2, 20, 15),
    (3, 30, 25)
]
records_df = spark.createDataFrame(records, ["id", "col1", "col2"])

rules_data = [
    (1, "col1 > col2"),
    (2, "col1 < col2"),
    (3, "col1 == col2")
]
rules_df = spark.createDataFrame(rules_data, ["rule_id", "sql_exp"])

# Cross join records with rules to get all combinations
combined_df = records_df.crossJoin(rules_df)


# Define a UDF to evaluate the expression
def evaluate_expr(expression, col1, col2):
    try:
        return eval(expression.replace("col1", str(col1)).replace("col1", str(col1)))
    except Exception as e:
        return False

evaluate_expr_udf = udf(evaluate_expr, BooleanType())


# Apply the rule evaluation using expr
result_df = combined_df.withColumn(
    "rule_passed",
    evaluate_expr_udf(col("sql_exp"), col("col1"), col("col2"))
)

# Show the results
result_df.show(truncate=False)

# Stop the SparkSession
spark.stop()
