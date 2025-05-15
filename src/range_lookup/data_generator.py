import random
from typing import List, Dict
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType
from pyspark.sql.functions import explode, sequence, col

COUNTRY_CITIES = {
    'USA': ['New York', 'Los Angeles', 'Chicago', 'Houston'],
    'UK': ['London', 'Manchester', 'Birmingham'],
    'Germany': ['Berlin', 'Munich', 'Hamburg'],
    'China': ['Beijing', 'Shanghai', 'Shenzhen'],
    'Japan': ['Tokyo', 'Osaka', 'Yokohama'],
    'Australia': ['Sydney', 'Melbourne', 'Brisbane']
}
ALL_COUNTRIES = list(COUNTRY_CITIES.keys())

def generate_location_flat() -> Dict:
    country = random.choice(ALL_COUNTRIES)
    city = random.choice(COUNTRY_CITIES.get(country, [f"City_{random.randint(1,1000)}"]))
    lat = random.uniform(-90, 90)
    long = random.uniform(-180, 180)
    return {
        'city': city,
        'country': country,
        'lat': lat,
        'long': long
    }

def generate_ip_ranges(num_ranges: int, start_ip: int = 0, end_ip: int = 2**32-1, spark: 'SparkSession' = None):
    data = []
    current = start_ip
    max_range_size = max(1, (end_ip - start_ip) // num_ranges)
    for _ in range(num_ranges):
        range_size = random.randint(1, max_range_size)
        first_num = current
        last_num = min(current + range_size, end_ip)
        location = generate_location_flat()
        row = {
            'first_num': first_num,
            'last_num': last_num,
        }
        row.update(location)
        data.append(row)
        current = last_num + 1
        if current > end_ip:
            break
    schema = StructType([
        StructField('first_num', LongType(), True),
        StructField('last_num', LongType(), True),
        StructField('city', StringType(), True),
        StructField('country', StringType(), True),
        StructField('lat', FloatType(), True),
        StructField('long', FloatType(), True),
    ])
    if spark is None:
        spark = SparkSession.builder.master('local[*]').appName('DataGen').getOrCreate()
    return spark.createDataFrame(data, schema=schema)

def generate_test_queries(num_queries: int, start_ip: int = 0, end_ip: int = 2**32-1) -> List[int]:
    return [random.randint(start_ip, end_ip) for _ in range(num_queries)]

def expand_ip_ranges_to_ipv4(df, spark):
    """
    Transform the generated IP range DataFrame to a new DataFrame with schema:
    ipv4_num, city, country, lat, long
    For each row, expand the range from first_num to last_num into individual rows.
    """
    expanded_df = df.withColumn(
        "ipv4_num", explode(sequence(col("first_num"), col("last_num")))
    ).select(
        "ipv4_num", "city", "country", "lat", "long"
    )
    return expanded_df

def main():
    import argparse
    random.seed(42)
    parser = argparse.ArgumentParser(description='Generate IP range data with PySpark')
    parser.add_argument('--num_ranges', type=int, default=1000, help='Number of IP ranges to generate')
    parser.add_argument('--num_queries', type=int, default=100, help='Number of test queries to generate')
    parser.add_argument('--start_ip', type=int, default=0, help='Start IP as integer (default: 0)')
    parser.add_argument('--end_ip', type=int, default=2**32-1, help='End IP as integer (default: 2^32-1)')
    args = parser.parse_args()

    spark = SparkSession.builder.master('local[*]').appName('DataGen').getOrCreate()
    print(f"Generating {args.num_ranges} IP ranges from {args.start_ip} to {args.end_ip}")
    df = generate_ip_ranges(args.num_ranges, start_ip=args.start_ip, end_ip=args.end_ip, spark=spark)
    print(f"Generated {df.count()} IP ranges. Sample:")
    df.show(5)

    expanded_df = expand_ip_ranges_to_ipv4(df, spark)
    print(f"\nExpanded IP ranges to IPv4. Sample:")
    expanded_df.show(5)

    queries = generate_test_queries(args.num_queries, start_ip=args.start_ip, end_ip=args.end_ip)
    print(f"\nGenerated {len(queries)} test queries. Sample:")
    print(queries[:5])
    spark.stop()

if __name__ == "__main__":
    main()
