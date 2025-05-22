from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, exists, broadcast, lit, expr, posexplode, array, create_map
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from itertools import chain

hashset_data = [
    {
        "MD5": "d41d8cd98f00b204e9800998ecf8427e",
        "SHA_1": "da39a3ee5e6b4b0d3255bfef95601890afd80709",
        "SHA_256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "SHA_512": "",
    },
    {
        "MD5": "098f6bcd4621d373cade4e832627b4f6",
        "SHA_1": "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
        "SHA_256": "",
        "SHA_512": "1f40fc92da241694750979ee6cf582f2d5d7d28e18335de05abc54d0560e0f5302860c652bf08d560252aa5e74210546f369fbbbce8c12cfc7957b2652fe9a75",
    },
    {
        "MD5": "",
        "SHA_1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
        "SHA_256": "521d24381b27c5cfca76201e27434d4b9a9868fd9179562f3f386d0763883bf7",
        "SHA_512": "b109f3bbbc244eb82441917ed06d618b9008dd09c7f1d8b0a7e7b5d8eff7f6b6eab8aef6e5e2b3c5c960b8e6a5c1b9e3e7b7b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8",
    },
]

activity_data = [
    {
        "actor": {
            "process": {
                "integrity_id": 5,
                "pid": 90504,
                "cmd_line": "",
                "file": {
                    "name": "dnscryptproxy.exe",
                    "type_id": 0,
                    "path": "C:\\Program Files (x86)\\Cisco\\Cisco Secure Client\\dnscryptproxy.exe",
                    "hashes": [
                        {
                            "algorithm_id": 3,
                            "value": "521d24381b27c5cfca76201e27434d4b9a9868fd9179562f3f386d0763883bf7",
                        }
                    ],
                },
                "name": "dnscryptproxy.exe",
                "user": {"name": "NT AUTHORITY\\SYSTEM", "type_id": 0},
            }
        }
    }
]

# Initialize Spark session
spark = SparkSession.builder.appName("LoadData").getOrCreate()

# Load hashset_data into a DataFrame
hashset_df = spark.createDataFrame(hashset_data)

# Define the schema for the nested hashes
hash_schema = ArrayType(
    StructType([
        StructField("algorithm_id", IntegerType(), True),
        StructField("value", StringType(), True),
    ])
)

# Define the schema for the nested file
file_schema = StructType([
    StructField("name", StringType(), True),
    StructField("type_id", IntegerType(), True),
    StructField("path", StringType(), True),
    StructField("hashes", hash_schema, True),
])

# Define the schema for the nested process
process_schema = StructType([
    StructField("integrity_id", IntegerType(), True),
    StructField("pid", IntegerType(), True),
    StructField("cmd_line", StringType(), True),
    StructField("file", file_schema, True),
    StructField("name", StringType(), True),
    StructField("user", StructType([
        StructField("name", StringType(), True),
        StructField("type_id", IntegerType(), True),
    ]), True)
])

# Define the schema for the activity
activity_schema = StructType([
    StructField("actor", StructType([
        StructField("process", process_schema, True)
    ]), True)
])

# Now create the DataFrame with the schema
activity_df = spark.createDataFrame(activity_data, schema=activity_schema)

# Show the DataFrames
# hashset_df.show(truncate=False)
# activity_df.show(truncate=False)
algorithm_id_map = {
    "MD5": 1,
    "SHA_1": 2,
    "SHA_256": 3,
    "SHA_512": 4,
    # Add more if needed
}
# Extract algorithm_id and value from actor.process.file.hashes[0]
activity_df2 = activity_df.select(
    "*",
    col("actor.process.file.hashes")[0].getField("algorithm_id").alias("hash_algorithm_id"),
    col("actor.process.file.hashes")[0].getField("value").alias("hash_value")
)

# Show the updated DataFrame
activity_df2.select("hash_algorithm_id", "hash_value").show(truncate=False)

# Step 1: Get all hash field names dynamically
hash_fields = ["MD5", "SHA_1", "SHA_256", "SHA_512"]

# Step 2: Unpivot (melt) the hashset_df
melted = hashset_df.select(
    posexplode(array(*[col(f) for f in hash_fields])).alias("algorithm_pos", "hash_value")
)
# print(hashset_df.show(truncate=False))
# Step 3: Add algorithm name and filter out empty values
# melted = melted.withColumn("algorithm_name", when(lit(True), lit(None)))
melted = melted.withColumn("algorithm_name", lit(None))
print(melted.show(truncate=False))

for i, name in enumerate(hash_fields):
    melted = melted.withColumn(
        "algorithm_name",
        when(col("algorithm_pos") == i, lit(name)).otherwise(col("algorithm_name"))
    )
melted = melted.filter(col("hash_value") != "")
print(melted.show(truncate=False))
# Step 4: Map algorithm_name to algorithm_id



mapping_expr = create_map([lit(x) for x in chain(*algorithm_id_map.items())])
melted = melted.withColumn("hash_algorithm_id", mapping_expr[col("algorithm_name")])

print(melted.show(truncate=False))
# Step 5: Now join as before
activity_df3 = activity_df2.alias("a").join(
    melted.alias("m"),
    (col("a.hash_algorithm_id") == col("m.hash_algorithm_id")) &
    (col("a.hash_value") == col("m.hash_value")),
    how="left"
).withColumn(
    "matches",
    col("m.hash_value").isNotNull() & col("m.algorithm_name").isNotNull()
)

# Add observables column using expr
observables_expr = """
    struct(
        'file.hash' as name,
        'Hash' as type,
        8 as type_id,
        hash_value as value
    ) 
"""
# activity_df3 = activity_df3.withColumn(
#     "observables",
#     expr(observables_expr)
# )

activity_df4 = activity_df3.select(
    col("a.hash_algorithm_id"),
    col("a.hash_value"),
    "matches"
).withColumn(
    "observables",
    expr(observables_expr)
)

print(activity_df4.show(truncate=False))



