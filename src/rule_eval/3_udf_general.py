from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct
from pyspark.sql.types import BooleanType, StructType, StructField, StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("UDF Expression Example") \
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

# Stop the SparkSession
spark.stop()
