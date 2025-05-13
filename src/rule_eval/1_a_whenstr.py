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

# Build when string from rules_data
when_str = "CASE "
for rule_id, sql_exp in rules_data:
    when_str += f"WHEN rule_id = {rule_id} THEN {sql_exp} "
when_str += "END"

# print(when_str)
# Cross join records with rules and evaluate rules using the when string
result_df = records_df.crossJoin(rules_df).withColumn(
    "rule_passed",
    expr(when_str)
)

# Show the results
result_df.show(truncate=False)
end_time = time.time()
print(f"Number of rows: {result_df.count()}")
print(f"Time taken: {end_time - start_time} seconds")
# Stop the SparkSession
spark.stop()