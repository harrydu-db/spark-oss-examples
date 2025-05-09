# JSON Parser example

## Problem: Loading JSON data efficiently 

The problem we're trying to solve is to load JSON data to DataFrame and renamed and casted the columns based my schema definition.

We have a list of json data with nested structure. We want to load the json, and flat the structure. 

### Example Data

Here is the content of json file. 

```json
{
    "eventId" : "fault000001",
    "time" : "2024-05-09T17:16:45.123Z",
    "priority" : "High",
    "sourceId" : "S527946875",
    "alarm" : 
    {
        "additionalInformation" : 
        {
            "alarmCode" : 2195630,
            "alarmDescription" : "When the equipped ALD does not exist in HDLC scan result.",
            "alarmSeverity" : "MAJOR"
        },
        "eventCategory" : "equipment-alarm"
    }
}
```

Here is the column mapping I want

```json
{
    "eventId": "EID",
    "time": "TIME",
    "priority": "P",
    "sourceId": "SOURCE",
    "alarm.additionalInformation.alarmCode": "AlarmCode",
    "alarm.additionalInformation.alarmDescription": "AlarmDESC",
    "alarm.additionalInformation.alarmSeverity": "AlarmSEV",
    "alarm.eventCategory": "EventCategory"
}
```

### Solution

We developed a solution that:
1. Uses PySpark to load and process the JSON data
2. Automatically handles type conversions
3. Flattens the nested structure
4. Renames columns according to the mapping

Here's the key parts of the solution:

```python
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
selected_columns = [col(key).cast(dtype).alias(alias) 
                   for key, (dtype, alias) in column_mapping.items()]

# Apply transformations
result_df = df.select(*selected_columns)
```

Key features of the solution:
1. **Automatic Type Conversion**: Handles type conversions using `cast`
2. **Clean Mapping**: Column mappings include both type and alias information
3. **Maintainable**: Easy to add new columns or modify existing ones

The solution handles:
- Nested JSON structures
- Different data types
- Column renaming
- Type conversions
- Null values
