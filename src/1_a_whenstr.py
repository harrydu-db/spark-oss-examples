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

# Build when string from rules_data
when_str = "CASE "
for rule_id, sql_exp in rules_data:
    when_str += f"WHEN rule_id = {rule_id} THEN {sql_exp} "
when_str += "END"

print(when_str)
# Cross join records with rules and evaluate rules using the when string
result_df = records_df.crossJoin(rules_df).withColumn(
    "rule_passed",
    expr(when_str)
)

# Show the results
result_df.show(truncate=False)

# Stop the SparkSession
spark.stop()