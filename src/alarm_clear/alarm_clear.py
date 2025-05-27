from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType


def clear_alarms(spark, records):
    """
    Given a list of (StartTime, EndTime, AlarmID), return a list of (StartTime, EndTime, AlarmID, Cleared)
    as described in the README.
    """
    schema = StructType([
        StructField("StartTime", IntegerType(), False),
        StructField("EndTime", IntegerType(), False),
        StructField("AlarmID", StringType(), False),
    ])
    df = spark.createDataFrame(records, schema)

    # Add Cleared column
    df = df.withColumn("Cleared", (F.col("StartTime") != F.col("EndTime")))

    # For each (AlarmID, StartTime), keep the row with Cleared=True if it exists, else the row with Cleared=False
    # We'll use a window to rank Cleared=True higher
    from pyspark.sql.window import Window

    w = Window.partitionBy("AlarmID", "StartTime").orderBy(F.desc("Cleared"))
    df = df.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") == 1).drop("rank")
    
    # Select and order columns as required
    df = df.select("StartTime", "EndTime", "AlarmID", "Cleared")

    return [tuple(row) for row in df.collect()]


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]").appName("AlarmClearExample").getOrCreate()
    records = [
        (1, 1, 'A'),
        (1, 2, 'A'),
        (1, 1, 'B'),
        (3, 3, 'A')
    ]
    results = clear_alarms(spark, records)
    for row in results:
        print(row)
    spark.stop()
