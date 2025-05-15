from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, expr
from data_generator import generate_ip_ranges, generate_test_queries
import time
if __name__ == "__main__":
    spark = SparkSession.builder.master('local[*]').appName('RangeJoin').getOrCreate()
    num_ranges = 1_000_000
    num_queries = 1_000
    start_ip = 0
    end_ip = 10_000_000

    # Generate IP ranges
    ip_ranges = generate_ip_ranges(num_ranges, end_ip=end_ip, spark=spark)

    # Print range coverage info
    total_ip_range = end_ip - start_ip + 1
    ip_ranges_with_size = ip_ranges.withColumn("range_size", col("last_num") - col("first_num") + 1)
    total_covered = ip_ranges_with_size.agg(spark_sum("range_size")).collect()[0][0]
    print(f"Total covered IPs: {total_covered}")
    print(f"Total IP range: {total_ip_range}")
    print(f"Coverage ratio: {total_covered / total_ip_range}")

    # Generate test queries
    queries = generate_test_queries(num_queries, start_ip=start_ip, end_ip=end_ip)
    query_df = spark.createDataFrame([(q,) for q in queries], ["ipv4_num"])
    start_time = time.time()
    # Perform range join
    joined = query_df.join(
        ip_ranges,
        (query_df.ipv4_num >= ip_ranges.first_num) & (query_df.ipv4_num <= ip_ranges.last_num),
        how="left"
    ).select(
        "ipv4_num", "city", "country", "lat", "long"
    )

    joined.show(10)
    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")

    spark.stop()
