from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit
from pyspark import SparkContext
sparkContext = SparkContext.getOrCreate()
sparkContext.setLogLevel("WARN")

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Expression Example") \
    .getOrCreate()

# Sample data
records = [
    (1, 10, 5),
    (2, 20, 15),
    (3, 30, 30)
]
records_df = spark.createDataFrame(records, ["id", "col1", "col2"])

rules_data = [
    (1, "col1 > col2"),
    (2, "col1 < col2"),
    (3, "col1 == col2")
]
rules_df = spark.createDataFrame(rules_data, ["rule_id", "sql_exp"])

# Build when clause dynamically
when_clause = when(lit(False), lit(False))  # Start with empty when clause
for rule_id, sql_exp in rules_data:
    when_clause = when_clause.when(col("rule_id") == rule_id, expr(sql_exp))

# Cross join records with rules and evaluate rules in one step
result_df = records_df.crossJoin(rules_df).withColumn(
    "rule_passed",
    when_clause
)

# Show the results
result_df.show(truncate=False)

# Stop the SparkSession
spark.stop()