from pyspark.sql.functions import *
from pyspark.sql.types import BooleanType
from pyspark import SparkContext
from pyspark.sql import SparkSession
import time
from data_generator import generate_sample_data, get_rules_data
import pandas as pd
import re

sparkContext = SparkContext.getOrCreate()
sparkContext.setLogLevel("WARN")

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Expression Example") \
    .getOrCreate()


# Pandas UDF that evaluates a dynamic expression per row
def make_eval_sql_expr_pd(record_columns):
    @pandas_udf(BooleanType())
    def eval_sql_expr_pd(*cols: pd.Series) -> pd.Series:
        *data_cols, expr_col = cols
        df = pd.concat(data_cols, axis=1)
        df.columns = record_columns
        results = []
        for i in range(len(df)):
            row = df.iloc[i]
            expr = expr_col.iloc[i]
            try:
                py_expr = sql_to_python_expr(expr)
                result = eval(py_expr, {}, row.to_dict())
                results.append(bool(result))
            except Exception:
                print(f"Error evaluating expression: {expr}")
                results.append(False)
        return pd.Series(results)
    return eval_sql_expr_pd

def sql_to_python_expr(expr: str) -> str:
    # Replace SQL operators with Python equivalents
    expr = re.sub(r'\bAND\b', 'and', expr, flags=re.IGNORECASE)
    expr = re.sub(r'\bOR\b', 'or', expr, flags=re.IGNORECASE)
    # expr = re.sub(r'=', '==', expr)
    # expr = re.sub(r'<>', '!=', expr)
    # Remove double '==' from already correct '==' (from '=' replacement)
    # expr = re.sub(r'==+', '==', expr)
    # Fix '==' in '!='
    # expr = expr.replace('!==', '!=')
    # Remove accidental '==' in '>=', '<='
    # expr = expr.replace('>==', '>=').replace('<==', '<=')
    return expr

# Generate data
records = generate_sample_data(100_000)
records_df = spark.createDataFrame(records, ["id", "col1", "col2", "col3", "col4"])

# Generate rules
rules_data = get_rules_data()
rules_df = spark.createDataFrame(rules_data, ["rule_id", "sql_exp"])

# Start time 
start_time = time.time()

# Cross join
joined_df = records_df.crossJoin(rules_df)

# Apply pandas UDF
# Note: pass all data columns and sql_exp explicitly
input_cols = records_df.columns + ["sql_exp"]
eval_sql_expr_pd = make_eval_sql_expr_pd(records_df.columns)
result_df = joined_df.withColumn(
    "rule_passed",
    eval_sql_expr_pd(*[col(c) for c in input_cols])
).filter(col("rule_passed") == True)

# Show results
print(f"Number of rows: {result_df.count()}")
end_time = time.time()
result_df.show(truncate=False)
print(f"Time taken: {end_time - start_time} seconds")

# Stop the SparkSession
spark.stop()