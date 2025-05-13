from pyspark.sql.functions import col, expr, when, lit
from pyspark import SparkContext
from pyspark.sql import SparkSession
import time
from data_generator import generate_sample_data, get_rules_data

sparkContext = SparkContext.getOrCreate()
sparkContext.setLogLevel("WARN")

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Expression Example") \
    .getOrCreate()

# generate data
records = generate_sample_data(100)
records_df = spark.createDataFrame(records, ["id", "col1", "col2", "col3", "col4"])

# generate rules
rules_data = get_rules_data()
rules_df = spark.createDataFrame(rules_data, ["rule_id", "sql_exp"])

# Start time 
start_time = time.time()

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
end_time = time.time()
print(f"Number of rows: {result_df.count()}")
print(f"Time taken: {end_time - start_time} seconds")

# Stop the SparkSession
spark.stop()