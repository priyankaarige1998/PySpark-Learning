# Day 11 – User Defined Functions (UDFs)

## Topics Covered

- What is a UDF?
- Creating a Python function
- Converting a Python function into a Spark UDF
- Applying a UDF using `withColumn()`
- Why built-in Spark functions are faster than UDFs

## Project Structure

```text
Day11_UDFs
│
├── data
│   └── employees.csv
├── udf_example.py
└── README.md
```

## Example

Python Function:

```python
def classify_salary(salary):
    if salary >= 80000:
        return "High"
    elif salary >= 60000:
        return "Medium"
    else:
        return "Low"
```

Convert to UDF:

```python
salary_udf = udf(
    classify_salary,
    StringType()
)
```

Apply:

```python
employee_df = employee_df.withColumn(
    "salary_category",
    salary_udf(col("salary"))
)
```