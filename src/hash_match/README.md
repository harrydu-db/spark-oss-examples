# Hash Match Example

This example demonstrates how to use PySpark to match file hash values from network activity logs against a known hashset, and enrich the activity data with information about the matched hashes.

## Overview

- **Pivot hashset data**: The hashset is transformed from a wide format (one row per file, multiple hash columns) to a long format (one row per hash value, with algorithm info).
- **Activity data**: Simulated network activity data contains file names, paths, and flat fields for `algorithm_id` and `hash_value`.
- **Matching**: The activity data is joined with the pivoted hashset to determine if the hash value exists in the known hashset, and to enrich the activity data with the associated information.

## Files

- `hash_match.py`: Main script containing the example logic.
- `README.md`: This file.

## How It Works

1. **Load hashset and activity data**: Example data is hardcoded for demonstration.
2. **Pivot hashset**: The `pivot_hashset` function transforms the hashset to a long format with columns: `hash_value`, `algorithm_name`, and `algorithm_id`.
3. **Prepare activity data**: The activity data is already flat, with `algorithm_id` and `hash_value` fields.
4. **Match and enrich**: The `enrich_activity` function joins the activity data with the pivoted hashset to add a `matched` boolean column.

## Example Output

The script prints the following DataFrames:

- The original hashset
- The pivoted (melted) hashset
- The enriched activity data, showing which activity hashes matched the hashset

Example output:

```
+----+-----+-------+-------+
|MD5 |SHA_1|SHA_256|SHA_512|
+----+-----+-------+-------+
|a   |b    |c      |       |
|d   |e    |       |f      |
|    |g    |h      |i      |
+----+-----+-------+-------+

+----------+-------------+------------+
|hash_value|algorithm_name|algorithm_id|
+----------+-------------+------------+
|a         |MD5          |1           |
|b         |SHA_1        |2           |
|c         |SHA_256      |3           |
|d         |MD5          |1           |
|e         |SHA_1        |2           |
|f         |SHA_512      |4           |
|g         |SHA_1        |2           |
|h         |SHA_256      |3           |
|i         |SHA_512      |4           |
+----------+-------------+------------+

+-------------------+-------------------------------+-------------+----------+-------+
|name               |path                           |algorithm_id |hash_value|matched|
+-------------------+-------------------------------+-------------+----------+-------+
|some_process.exe   |C:\somewhere\some_process.exe |3            |c         |true   |
+-------------------+-------------------------------+-------------+----------+-------+
```

## How to Run

1. Make sure you have [PySpark](https://spark.apache.org/docs/latest/api/python/) installed:
   ```bash
   pip install pyspark
   ```
2. Run the script:
   ```bash
   python hash_match.py
   ```

## Customization

- You can modify the `hashset_data` and `activity_data` variables in `hash_match.py` to test with your own data.
- The join type in `enrich_activity` can be changed to `inner` or `left` depending on whether you want to see only matches or all activity records.

## License

This example is provided for educational purposes.
