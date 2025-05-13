import random

def generate_sample_data(num_rows, min_val=1, max_val=100):
    """
    Generate sample data with random values for all columns except id.
    
    Args:
        num_rows (int): Number of rows to generate
        min_val (int): Minimum value for random numbers
        max_val (int): Maximum value for random numbers
        
    Returns:
        list: List of tuples containing the generated data
    """
    records = []
    for i in range(1, num_rows + 1):
        col1 = random.randint(min_val, max_val)
        col2 = random.randint(min_val, max_val)
        col3 = random.randint(min_val, max_val)
        col4 = random.randint(min_val, max_val)
        records.append((i, col1, col2, col3, col4))
    return records

def get_rules_data():
    """
    Returns the predefined rules for evaluation.
    
    Returns:
        list: List of tuples containing rule_id and SQL expressions
    """
    return [
        (1, "col1 > col2"),
        (2, "col1 < col2"),
        (3, "col1 == col2"),
        (4, "col3 > col4"),
        (5, "col1 + col2 > col3"),
        (6, "col4 - col3 < col2"),
        (7, "col1 * col2 > col3 * col4"),
        (8, "col1 + col2 + col3 > col4"),
        (9, "col1 > col2 AND col3 > col4"),
        (10, "col1 < col2 OR col3 < col4")
    ]

