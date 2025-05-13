from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit
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

# Collect rules to process them one by one
rules_list = rules_df.collect()

start_time = time.time()

# Initialize an empty list to store result DataFrames
result_dfs = []

# Process each rule
for rule in rules_list:
    rule_id = rule["rule_id"]
    sql_exp = rule["sql_exp"]
    
    # Create a result DataFrame for this rule
    rule_result_df = records_df.select(
        "*",
        lit(rule_id).alias("rule_id"),
        lit(sql_exp).alias("sql_exp"),
        expr(sql_exp).alias("rule_passed")
    )
    
    result_dfs.append(rule_result_df)

# Union all result DataFrames
final_results = result_dfs[0]
for df in result_dfs[1:]:
    final_results = final_results.union(df)

# Show the results in a structured format
final_results.show(truncate=False)

end_time = time.time()

print(f"Number of rows: {final_results.count()}")
print(f"Time taken: {end_time - start_time} seconds")

# Stop the SparkSession
spark.stop()
