# The Problem

There is a huge table (geoip table) with 4 trillion rows. It has ip range to geo location mapping. With the following schema: 
(first_num and last_num makes a range. There is no overlapping.  The first number and last_num are ordered.)

```
 |-- first_num: bigint (nullable = true)
 |-- last_num: bigint (nullable = true)
 |-- city: string (nullable = true)
 |-- country: string (nullable = true)
 |-- lat: float (nullable = true)
 |-- long: float (nullable = true)
```

The ipv4 address "a.b.c.d" is converted to a ipv4_num by converting the 32 digit binary to a bigint. I want to do a quick lookup of geoip table to find the geo location of the ipv4 address. 

Currently they are using range-join to lookup the location for the incoming network activity ip. 

I want to see how can I make the lookup as fast as possible in Spark, with optimal data structure for both storage and performance. 

---

# Implemented Lookup Methods

## 1. Range Join (1_range_join.py)
- Performs a join between the query IPs and the geoip table using a range condition:
  `(query_df.ipv4_num >= ip_ranges.first_num) & (query_df.ipv4_num <= ip_ranges.last_num)`
- This is the classic approach but can be slow for very large tables, as it requires a range comparison for each query.
- The script also prints the coverage ratio of the generated ranges over the total IP space.

## 2. Direct IP Lookup (2_ip_lookup.py)
- Expands the IP ranges into a flat mapping of every possible `ipv4_num` to its location (using `expand_ip_ranges_to_ipv4`).
- Performs a direct join on `ipv4_num` for fast lookups.
- Prints the sparsity ratio of the mapping (how much of the IP space is actually covered).
- This method is very fast for lookups but can be memory-intensive if the mapping is large.

## 3. Range-Partitioned IP Lookup (3_ip_lookup_with_range_partition.py)
- Similar to the direct IP lookup, but partitions the mapping and queries by IP range for better parallelism and data locality.
- The number of partitions is set to 3 times the number of CPU cores.
- Joins are performed on both `ipv4_num` and a calculated `partition_id`.
- Prints the sparsity ratio of the mapping.
- This method can offer better performance and scalability for large datasets.

---

# Performance Testing

**Important:**
- The actual performance of each method depends on your data size, cluster configuration, and Spark environment.
- You should test these methods in your real environment with production-like data to obtain accurate performance numbers and choose the best approach for your use case.
