from pyspark.sql import SparkSession
from data_generator import generate_ip_ranges, generate_test_queries, expand_ip_ranges_to_ipv4
import time
if __name__ == "__main__":
    spark = SparkSession.builder.master('local[*]').appName('IPLookup').getOrCreate()
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

    # Generate test queries
    queries = generate_test_queries(num_queries, start_ip=start_ip, end_ip=end_ip)
    query_df = spark.createDataFrame([(q,) for q in queries], ["ipv4_num"])

    # For performance, cache or persist the mapping if reused
    ipv4_map.cache()

    start_time = time.time()
    # Perform direct join on ipv4_num
    joined = query_df.join(
        ipv4_map,
        on="ipv4_num",
        how="left"
    ).select(
        "ipv4_num", "city", "country", "lat", "long"
    )

    joined.show(10)
    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")

    spark.stop()

# Note: For even faster lookups on large datasets, consider partitioning or bucketing the ipv4_map DataFrame by 'ipv4_num' before writing to disk (e.g., using DataFrame.write.bucketBy or partitionBy in Spark SQL). This allows Spark to more efficiently prune files during the join. In-memory, caching is the main optimization for this use case.
