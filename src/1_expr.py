from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
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

# Apply the rule evaluation using expr
result_df = combined_df.withColumn(
    "rule_passed",
    expr("sql_exp")
)



# Show the results
result_df.show(truncate=False)

# Stop the SparkSession
spark.stop()