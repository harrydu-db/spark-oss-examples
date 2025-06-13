from pyspark.sql.functions import col, expr, when, lit, udf, struct
from pyspark.sql.types import BooleanType
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
# General-purpose UDF that evaluates sql_exp with all columns in the row
@udf(BooleanType())
def eval_sql_expr(row_data, sql_exp):
    try:
        # Convert Row to dictionary to use as variable context
        context = row_data.asDict()
        return eval(sql_exp, {}, context)
    except Exception:
        return False  # Fallback for invalid expressions

# Generate data
records = generate_sample_data(5)
records_df = spark.createDataFrame(records, ["id", "col1", "col2", "col3", "col4"])

# Generate rules
rules_data = get_rules_data()
rules_df = spark.createDataFrame(rules_data, ["rule_id", "sql_exp"])

# Cross join and evaluate rules
result_df = records_df.crossJoin(rules_df).withColumn(
    "rule_passed",
    eval_sql_expr(struct([col(c) for c in records_df.columns]), col("sql_exp"))
)

# Show results
print(f"Number of rows: {result_df.count()}")
result_df.show(truncate=False)

# Stop the SparkSession
spark.stop()