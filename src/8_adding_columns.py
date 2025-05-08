from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, explode, array, struct, lit
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

# Start with the original records dataframe
result_df = records_df

# Add a column for each rule evaluation
for rule_id, sql_exp in rules_data:
    result_df = result_df.withColumn(
        f"rule_{rule_id}", 
        expr(sql_exp)
    )

# Create an array of structs for each rule
rule_structs = [
    struct(
        lit(rule_id).alias("rule_id"),
        lit(sql_exp).alias("sql_exp"),
        col(f"rule_{rule_id}").alias("rule_passed")
    )
    for rule_id, sql_exp in rules_data
]

# Add the array column and explode it
result_df = result_df.withColumn("rules", array(*rule_structs)) \
    .select("id", "col1", "col2", explode("rules").alias("rule")) \
    .select("id", "col1", "col2", "rule.rule_id", "rule.sql_exp", "rule.rule_passed")

# Show the results
result_df.show(truncate=False)

# Stop the SparkSession
spark.stop()