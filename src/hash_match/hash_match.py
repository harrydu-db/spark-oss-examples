from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, array, create_map, explode, struct
from itertools import chain

# Example hashset and activity data
hashset_data = [
    {"MD5": "a", "SHA_1": "b", "SHA_256": "c", "SHA_512": ""},
    {"MD5": "d", "SHA_1": "e", "SHA_256": "", "SHA_512": "f"},
    {"MD5": "", "SHA_1": "g", "SHA_256": "h", "SHA_512": "i"},
]

activity_data = [
    {
        "name": "some_process.exe",
        "path": "C:\\somewhere\\some_process.exe",
        "algorithm_id": 3,
        "hash_value": "c",
    }
]

# Pivot hashset to long format
def pivot_hashset(df):
    algorithm_id_map = {"MD5": 1, "SHA_1": 2, "SHA_256": 3, "SHA_512": 4}
    mapping_expr = create_map([lit(x) for x in chain(*algorithm_id_map.items())])
    return (
        df.select(
            explode(array(
                struct(col("MD5").alias("hash_value"), lit("MD5").alias("algorithm_name")),
                struct(col("SHA_1").alias("hash_value"), lit("SHA_1").alias("algorithm_name")),
                struct(col("SHA_256").alias("hash_value"), lit("SHA_256").alias("algorithm_name")),
                struct(col("SHA_512").alias("hash_value"), lit("SHA_512").alias("algorithm_name")),
            )).alias("hash_struct")
        )
        .select("hash_struct.hash_value", "hash_struct.algorithm_name")
        .filter("hash_value != ''")
        .withColumn("algorithm_id", mapping_expr[col("algorithm_name")])
    )

# Enrich activity with match info
def enrich_activity(df, pivoted_df):
    return (
        df.join(pivoted_df, df.hash_value == pivoted_df.hash_value, how="left")
          .select(
              "*",
              (pivoted_df.hash_value.isNotNull()).alias("matched")
          )
    )

spark = SparkSession.builder.appName("HashMatch").getOrCreate()
hashset_df = spark.createDataFrame(hashset_data)
activity_df = spark.createDataFrame(activity_data)

print("Original hashset:")
hashset_df.show(truncate=False)

melted = pivot_hashset(hashset_df)
print("Pivoted hashset:")
melted.show(truncate=False)

enriched = enrich_activity(activity_df, melted)
print("Enriched activity:")
enriched.show(truncate=False)
