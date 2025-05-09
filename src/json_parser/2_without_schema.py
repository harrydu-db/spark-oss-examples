from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cast
from pyspark.sql.types import IntegerType, StringType, TimestampType
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
                "alarmCode": 2195630,
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
                "alarmCode": 2195630,
                "alarmDescription": "When the equipped ALD does not exist in HDLC scan result.",
                "alarmSeverity": "MAJOR"
            },
            "eventCategory": "equipment-alarm"
        }
    }
]

# Convert Python list to JSON string
json_str = json.dumps(sample_json)

# Create DataFrame from JSON string
df = spark.read.json(spark.sparkContext.parallelize([json_str]))

df.printSchema()

# Define column mappings with type objects
column_mapping = {
    "eventId": (StringType(), "EID"),
    "time": (TimestampType(), "TIME"),
    "priority": (StringType(), "P"),
    "sourceId": (StringType(), "SOURCE"),
    "alarm.additionalInformation.alarmCode": (IntegerType(), "AlarmCode"),
    "alarm.additionalInformation.alarmDescription": (StringType(), "AlarmDESC"),
    "alarm.additionalInformation.alarmSeverity": (StringType(), "AlarmSEV"),
    "alarm.eventCategory": (StringType(), "EventCategory")
}

# Create column expressions with type conversion
selected_columns = [col(key).cast(dtype).alias(alias) for key, (dtype, alias) in column_mapping.items()]

result_df = df.select(*selected_columns)

result_df.printSchema()
# Show the transformed data
result_df.show(truncate=False)

# Stop Spark session
spark.stop() 