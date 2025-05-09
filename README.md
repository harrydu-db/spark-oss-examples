# Spark OSS Examples

This repository contains various examples demonstrating different approaches and best practices for using Apache Spark. The examples are designed to help developers understand and implement common Spark patterns and use cases.

## Project Structure

```
.
├── src / rule_eval/           # Examples of different approaches for dynamic SQL expression evaluation
│   ├── 1_udf.py        # Basic UDF implementation
│   ├── 2_pandas_udf.py # Pandas UDF implementation
│   └── ...
└── src/ ...                # Other Spark examples
    ├── ...
    └── ...
```

## Rule Evaluation Examples

The `rule_eval` directory contains different implementations for evaluating dynamic SQL expressions in Spark. Each example demonstrates a different approach:


## Getting Started

### Prerequisites

- Python 3.7+
- Apache Spark 3.0+
- PySpark

### Installation

Install dependencies:
```bash
pip install -r requirements.txt
```
Apache Spark is also required to run the code. 

### Running Examples

To run any example, use the following command:

```bash
python src/[path]/[example].py
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

