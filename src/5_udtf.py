from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
from pyspark.sql.types import BooleanType, StructType, StructField, StringType, IntegerType, Row
from pyspark.sql.udtf import udtf

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("UDTF Expression Example") \
    .getOrCreate()

# Define column names
RECORD_COLUMNS = ["id", "col1", "col2"]

# Sample data
records = [
    (1, 10, 5),
    (2, 20, 15),
    (3, 30, 25)
]
records_df = spark.createDataFrame(records, RECORD_COLUMNS)

# Define rules as a list of tuples (rule_id, expression)
RULES = [
    (1, "col1 > col2"),
    (2, "col1 < col2"),
    (3, "col1 == col2")
]

# Define the UDTF class with annotation
@udtf(
    returnType=StructType([
        StructField("id", IntegerType()),
        StructField("col1", IntegerType()),
        StructField("col2", IntegerType()),
        StructField("rule_id", IntegerType()),
        StructField("sql_exp", StringType()),
        StructField("rule_passed", BooleanType())
    ])
)
class RuleEvaluator:
    def __init__(self):
        self.rules = RULES
    
    def eval(self, id: int, col1: int, col2: int):
        for rule_id, expr in self.rules:
            # Evaluate the expression
            rule_passed = eval(expr)
            yield (id, col1, col2, rule_id, expr, rule_passed)

# Apply the UDTF
result_df = records_df.selectExpr("rule_evaluator(id, col1, col2) as result") \
    .select("result.*")

# Show the results
result_df.show(truncate=False)

# +---+----+----+-------+------------+-----------+                                
# |id |col1|col2|rule_id|sql_exp     |rule_passed|
# +---+----+----+-------+------------+-----------+
# |1  |10  |5   |1      |col1 > col2 |true       |
# |1  |10  |5   |2      |col1 < col2 |false      |
# |1  |10  |5   |3      |col1 == col2|false      |
# |2  |20  |15  |1      |col1 > col2 |true       |
# |2  |20  |15  |2      |col1 < col2 |false      |
# |2  |20  |15  |3      |col1 == col2|false      |
# |3  |30  |25  |1      |col1 > col2 |true       |
# |3  |30  |25  |2      |col1 < col2 |false      |
# |3  |30  |25  |3      |col1 == col2|false      |
# +---+----+----+-------+------------+-----------+

# Stop the SparkSession
spark.stop()
