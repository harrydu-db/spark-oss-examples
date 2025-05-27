# Alarm Clear Problem

## Problem Statement

Given a list of alarm event records, each with the following fields:
- `StartTime`: The time the alarm started
- `EndTime`: The time the alarm ended (if the alarm is still active, `EndTime` equals `StartTime`)
- `AlarmID`: The identifier for the alarm

The goal is to:
1. Add a column indicating whether the alarm has been cleared (`Cleared`: `True` if the alarm has ended, `False` otherwise).
2. For each unique alarm event (defined by `AlarmID` and `StartTime`), keep only one row in the output.

## Input Format
A list of records, where each record is a tuple:
```
(StartTime, EndTime, AlarmID)
```
- If `StartTime == EndTime`, the alarm is active (not cleared).
- If `StartTime != EndTime`, the alarm has been cleared (ended).

## Output Format
A list of records, where each record is a tuple:
```
(StartTime, EndTime, AlarmID, Cleared)
```
- `Cleared` is a boolean: `True` if the alarm has ended, `False` otherwise.
- Only one row per unique (`AlarmID`, `StartTime`) is kept.

## Example

### Input
```python
records = [
    (1, 1, 'A'),   # Alarm A started at 1, not cleared
    (1, 2, 'A'),   # Alarm A started at 1, cleared at 2
    (1, 1, 'B'),   # Alarm B started at 1, not cleared
    (3, 3, 'A')    # Alarm A started at 3, not cleared
]
```

### Output
```python
results = [
    (1, 2, 'A', True),   # Alarm A started at 1, cleared at 2
    (1, 1, 'B', False),  # Alarm B started at 1, not cleared
    (3, 3, 'A', False)   # Alarm A started at 3, not cleared
]
```

## Notes
- For each (`AlarmID`, `StartTime`), if there is a record with `StartTime != EndTime`, it means the alarm was cleared, and that record should be kept with `Cleared=True`.
- If only `StartTime == EndTime` records exist for a given (`AlarmID`, `StartTime`), keep one such record with `Cleared=False`.





