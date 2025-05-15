import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from data_generator import generate_ip_ranges
from pyspark.sql import SparkSession

class TestDataGenerator(unittest.TestCase):
    def test_no_overlapping_ranges(self):
        num_ranges = 1000
        end_ip = 10000
        spark = SparkSession.builder.master('local[*]').appName('TestDataGen').getOrCreate()
        df = generate_ip_ranges(num_ranges, end_ip=end_ip, spark=spark)
        # Collect data to driver and sort by first_num
        data = df.select('first_num', 'last_num').collect()
        data_sorted = sorted(data, key=lambda row: row['first_num'])
        # Check for overlaps
        for i in range(1, len(data_sorted)):
            prev_last = data_sorted[i-1]['last_num']
            curr_first = data_sorted[i]['first_num']
            self.assertLess(prev_last, curr_first, f"Overlap detected between rows {i-1} and {i}")
        spark.stop()

if __name__ == "__main__":
    unittest.main() 