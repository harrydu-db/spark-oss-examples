from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
from pyspark.sql.types import BooleanType, StructType, StructField, StringType
from pyspark.sql.functions import udf
import re

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

# Define a UDF to evaluate the expression using annotation
@udf(returnType=BooleanType())
def evaluate_expr(expression: str, column_values: dict) -> bool:
    try:
        # Convert the struct to a dictionary
        values = column_values.asDict()
        
        # Create a pattern that matches any column name
        pattern = '|'.join(RECORD_COLUMNS)

        # print(f"Expression: {pattern}")
        
        # Replace all column names with their values in a single pass
        def replace_match(match):
            return str(values[match.group(0)])
            
        expr = re.sub(pattern, replace_match, expression)
        return eval(expr)
    except Exception as e:
        print(f"Error evaluating expression: {e}")
        return False

# Apply the rule evaluation using expr
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
