from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import json

# Create Spark session
spark = SparkSession.builder \
    .appName("JSON Parser") \
    .getOrCreate()

# Sample JSON data
sample_json = [
    {
        "eventId": "fault000001",
        "time": "2024-05-09T17:16:45.123Z",
        "priority": "High",
        "sourceId": "S527946875",
        "alarm": {
            "additionalInformation": {
                "alarmCode": "2195630",
                "alarmDescription": "When the equipped ALD does not exist in HDLC scan result.",
                "alarmSeverity": "MAJOR"
            },
            "eventCategory": "equipment-alarm"
        }
    },
    {
        "eventId": "fault000002",
        "time": "2024-05-09T17:16:46.456Z",
        "priority": "High",
        "sourceId": "S527946875",
        "alarm": {
            "additionalInformation": {
                "alarmCode": "2195630",
                "alarmDescription": "When the equipped ALD does not exist in HDLC scan result.",
                "alarmSeverity": "MAJOR"
            },
            "eventCategory": "equipment-alarm"
        }
    }
]

# Convert Python list to JSON string
json_str = json.dumps(sample_json)

# Define the schema for the JSON data
schema = StructType([
    StructField("eventId", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("priority", StringType(), True),
    StructField("sourceId", StringType(), True),
    StructField("alarm", StructType([
        StructField("additionalInformation", StructType([
            StructField("alarmCode", StringType(), True),
            StructField("alarmDescription", StringType(), True),
            StructField("alarmSeverity", StringType(), True)
        ]), True),
        StructField("eventCategory", StringType(), True)
    ]), True)
])

# Create DataFrame from JSON string
df = spark.read.schema(schema).json(spark.sparkContext.parallelize([json_str]))

# Show the raw data
print("Raw JSON data:")
df.show(truncate=False)

# Select and rename columns according to the mapping
column_mapping = {
    "eventId": "EID",
    "time": "TIME",
    "priority": "P",
    "sourceId": "SOURCE",
    "alarm.additionalInformation.alarmCode": "AlarmCode",
    "alarm.additionalInformation.alarmDescription": "AlarmDESC",
    "alarm.additionalInformation.alarmSeverity": "AlarmSEV",
    "alarm.eventCategory": "EventCategory"
}

selected_columns = [col(key).alias(value) for key, value in column_mapping.items()]
result_df = df.select(*selected_columns)

# Show the transformed data
print("\nTransformed data:")
result_df.show(truncate=False)

# Stop Spark session
spark.stop() 