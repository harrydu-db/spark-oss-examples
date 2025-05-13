from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, explode, array, struct, lit
from pyspark import SparkContext
from data_generator import generate_sample_data, get_rules_data
import time
sparkContext = SparkContext.getOrCreate()
sparkContext.setLogLevel("WARN")

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Expression Example") \
    .getOrCreate()

# Get records and rules data
records = generate_sample_data(num_rows=100)
records_df = spark.createDataFrame(records, ["id", "col1", "col2", "col3", "col4"])

rules_data = get_rules_data()
rules_df = spark.createDataFrame(rules_data, ["rule_id", "sql_exp"])

start_time = time.time()

# Create an array of structs for each rule with direct evaluation
rule_structs = [
    struct(
        lit(rule_id).alias("rule_id"),
        lit(sql_exp).alias("sql_exp"),
        expr(sql_exp).alias("rule_passed")
    )
    for rule_id, sql_exp in rules_data
]

# Add the array column and explode it
result_df = records_df.withColumn("rules", array(*rule_structs)) \
    .select("id", "col1", "col2", "col3", "col4", explode("rules").alias("rule")) \
    .select("id", "col1", "col2", "col3", "col4", "rule.rule_id", "rule.sql_exp", "rule.rule_passed")

# Show the results
result_df.show(truncate=False)
end_time = time.time()

print(f"Number of rows: {result_df.count()}")
print(f"Time taken: {end_time - start_time} seconds")   
# Stop the SparkSession
spark.stop()