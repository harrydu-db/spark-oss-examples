from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import LongType
from data_generator import generate_ip_ranges, generate_test_queries, expand_ip_ranges_to_ipv4
import time

# import multiprocessing

if __name__ == "__main__":
    # Calculate number of partitions based on cores
    # num_cores = multiprocessing.cpu_count()
    num_cores = 8
    num_partitions = num_cores * 3
    
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('IPLookupWithRangePartition') \
        .getOrCreate()
    
    num_ranges = 1_000_000
    num_queries = 1_000
    start_ip = 0
    end_ip = 10_000_000

    # Generate IP ranges
    ip_ranges = generate_ip_ranges(num_ranges, end_ip=end_ip, spark=spark)

    # Expand to direct ipv4_num to location mapping
    ipv4_map = expand_ip_ranges_to_ipv4(ip_ranges, spark)

    # Print sparsity info
    ipv4_map_count = ipv4_map.count()
    total_ip_range = end_ip - start_ip + 1
    print(f"ipv4_map count: {ipv4_map_count}")
    print(f"Total IP range: {total_ip_range}")
    print(f"Sparsity (ratio): {ipv4_map_count / total_ip_range}")

    # Calculate partition size
    partition_size = (end_ip + 1) // num_partitions

    # Add partition column to ipv4_map
    ipv4_map = ipv4_map.withColumn(
        "partition_id", 
        (col("ipv4_num") / partition_size).cast(LongType())
    )

    # Repartition the data
    ipv4_map = ipv4_map.repartition(num_partitions, "partition_id")

    # Cache the partitioned data
    ipv4_map.cache()

    # Generate test queries
    queries = generate_test_queries(num_queries, start_ip=0, end_ip=end_ip)
    query_df = spark.createDataFrame([(q,) for q in queries], ["ipv4_num"])

    # Add partition column to query DataFrame
    query_df = query_df.withColumn(
        "partition_id",
        (col("ipv4_num") / partition_size).cast(LongType())
    )

    start_time = time.time()
    # Perform partitioned join
    joined = query_df.join(
        ipv4_map,
        ["ipv4_num", "partition_id"],  # Join on both ipv4_num and partition_id
        how="left"
    ).select(
        "ipv4_num", "city", "country", "lat", "long"
    )
    joined.show(10)
    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")
    

    # Optional: Write partitioned data to disk for future use
    # ipv4_map.write.partitionBy("partition_id").parquet("path/to/partitioned_data")

    spark.stop()

# Note: For even faster lookups on large datasets, consider partitioning or bucketing the ipv4_map DataFrame by 'ipv4_num' before writing to disk (e.g., using DataFrame.write.bucketBy or partitionBy in Spark SQL). This allows Spark to more efficiently prune files during the join. In-memory, caching is the main optimization for this use case.
