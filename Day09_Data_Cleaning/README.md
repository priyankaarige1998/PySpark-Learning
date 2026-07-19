# Day 09 – Data Cleaning with PySpark

This project demonstrates common data-cleaning operations using PySpark DataFrames.

## Topics covered

* Identifying NULL values
* Using `isNull()`
* Using `isNotNull()`
* Removing NULL values with `dropna()`
* Replacing NULL values with `fillna()`
* Removing duplicate records
* Removing duplicates based on selected columns
* Converting column data types using `cast()`
* Converting string values to dates using `to_date()`
* Building a complete data-cleaning pipeline

## Project structure

```text
Day09_Data_Cleaning
│
├── data
│   └── employees.csv
│
├── data_cleaning.py
└── README.md
```

## Input data issues

The sample employee dataset contains:

* A missing salary
* A missing employee name
* A duplicate employee record
* A joining date stored as a string

## Main cleaning operations

### Fill missing values

```python
employee_df.fillna({
    "salary": 0,
    "name": "Unknown"
})
```

### Remove duplicate employee IDs

```python
employee_df.dropDuplicates(["employee_id"])
```

### Convert salary to double

```python
col("salary").cast("double")
```

### Convert joining date to date type

```python
to_date(
    col("joining_date"),
    "dd-MM-yyyy"
)
```

## How to run

Open the terminal inside the `Day09_Data_Cleaning` folder and run:

```powershell
python data_cleaning.py
```

## Expected final schema

```text
root
 |-- employee_id: integer
 |-- name: string
 |-- department: string
 |-- salary: double
 |-- joining_date: date
```
