# Spark OSS Examples

This repository contains examples of using Apache Spark for various data processing tasks.

## Problem: Rule Evaluation on DataFrame

The problem we're trying to solve is evaluating multiple rules (conditions) on each row of a DataFrame and presenting the results in a structured format. Specifically:

1. We have a DataFrame with records containing numeric columns
2. We have a set of rules defined as SQL expressions
3. We want to evaluate each rule against each record
4. We want to present the results showing which rules passed/failed for each record

### Example Data

```python
# Records DataFrame
records = [
    (1, 10, 5),   # id, col1, col2
    (2, 20, 15),
    (3, 30, 30)
]

# Rules DataFrame
rules_data = [
    (1, "col1 > col2"),
    (2, "col1 < col2"),
    (3, "col1 == col2")
]
```

## Solution Approaches

We explore several approaches to solve this problem, each with its own advantages and limitations:

### 1. Direct Expression Evaluation (`1_expr.py`)
```python
result_df = combined_df.withColumn(
    "rule_passed",
    expr("sql_exp")
)
```
This approach doesn't work yet. (Spark bug?).

### 2. Basic UDF (`2_udf.py`)
```python
def evaluate_expr(expression, col1, col2):
    try:
        return eval(expression.replace("col1", str(col1)).replace("col2", str(col2)))
    except Exception as e:
        return False

evaluate_expr_udf = udf(evaluate_expr, BooleanType())
```
This approach:
- Uses a simple UDF to evaluate expressions
- Hardcodes column names in the replacement
- Limited to specific columns (col1, col2)

Drawbacks:
- Hardcoded column names make it inflexible for different schemas
- Simple string replacement can be fragile:
  - If column names are substrings of each other (e.g., "col" and "col1"), replacements can be incorrect
  - If column names appear in string literals, they might be incorrectly replaced
  - No handling of column names that might contain special characters
- Requires code changes to add new columns
- No type safety for column values

### 3. General UDF (`3_udf_general.py`)
```python
def evaluate_expr(expression, column_values):
    try:
        expr = expression
        for col_name in RECORD_COLUMNS:
            if col_name in expr:
                expr = expr.replace(col_name, str(column_values[col_name]))
        return eval(expr)
    except Exception as e:
        return False
```
This approach:
- More flexible with column names
- Uses a struct to pass all column values
- Handles any number of columns

Drawbacks:
- Simple string replacement can be fragile:
  - If column names are substrings of each other (e.g., "col" and "col1"), replacements can be incorrect
  - If column names appear in string literals, they might be incorrectly replaced
  - No handling of column names that might contain special characters
- No type safety for column values
- Potential performance impact from multiple string replacements
- No validation of expression syntax before evaluation

### 4. UDF with Type Annotations (`4_udf_annotation.py`)
```python
@udf(returnType=BooleanType())
def evaluate_expr(expression: str, column_values: dict) -> bool:
    try:
        expr = expression
        for col_name in RECORD_COLUMNS:
            if col_name in expr:
                expr = expr.replace(col_name, str(column_values[col_name]))
        return eval(expr)
    except Exception as e:
        return False
```
This approach:
- Uses Python type hints for better code clarity
- Same functionality as the general UDF
- Better IDE support and type checking

### 5. UDTF 

TBD. Not working yet.

### 6. UDF with Regex (`6_udf_expr_regex.py`)
```python
@udf(returnType=BooleanType())
def evaluate_expr(expression: str, column_values: dict) -> bool:
    try:
        pattern = '|'.join(RECORD_COLUMNS)
        def replace_match(match):
            return str(column_values[match.group(0)])
        expr = re.sub(pattern, replace_match, expression)
        return eval(expr)
    except Exception as e:
        return False
```
This approach:
- Uses regex for more efficient column name replacement
- Handles column names that might be substrings of each other
- More robust than simple string replacement

### 7. Adding Columns (`8_adding_columns.py`)
```python
# First approach: Individual columns
for rule_id, sql_exp in rules_data:
    result_df = result_df.withColumn(
        f"rule_{rule_id}", 
        expr(sql_exp)
    )

# Second approach: Exploded results
rule_structs = [
    struct(
        lit(rule_id).alias("rule_id"),
        lit(sql_exp).alias("sql_exp"),
        col(f"rule_{rule_id}").alias("rule_passed")
    )
    for rule_id, sql_exp in rules_data
]
```
This approach:
- Creates separate columns for each rule
- Can be transformed into a long format using structs and explode
- More efficient than UDFs for simple expressions

### 8. Single Column Solution (`9_add_one_column.py`)
```python
rule_structs = [
    struct(
        lit(rule_id).alias("rule_id"),
        lit(sql_exp).alias("sql_exp"),
        expr(sql_exp).alias("rule_passed")
    )
    for rule_id, sql_exp in rules_data
]

# Add the array column and explode it
result_df = records_df.withColumn("rules", array(*rule_structs)) \
    .select("id", "col1", "col2", explode("rules").alias("rule")) \
    .select("id", "col1", "col2", "rule.rule_id", "rule.sql_exp", "rule.rule_passed")
    
```
This approach:
- Combines all rule evaluations into a single column
- More efficient than creating multiple columns
- Directly evaluates expressions without intermediate columns

## Usage

To run any of the examples:

```bash
python src/<example_file>.py
```

## Requirements

- Python 3.x
- PySpark
- Apache Spark