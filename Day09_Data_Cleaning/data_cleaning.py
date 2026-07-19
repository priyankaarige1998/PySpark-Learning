from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Create SparkSession

spark = (
SparkSession.builder
.appName("Day 09 - Data Cleaning")
.getOrCreate()
)

# Reduce unnecessary Spark messages

spark.sparkContext.setLogLevel("ERROR")

# Read the CSV file

employee_df = (
spark.read
.option("header", True)
.option("inferSchema", True)
.csv("data/employees.csv")
)

print("\nORIGINAL DATA")
employee_df.show()

print("\nORIGINAL SCHEMA")
employee_df.printSchema()

# ---------------------------------------------------

# 1. Find rows where salary is NULL

# ---------------------------------------------------

print("\nROWS WHERE SALARY IS NULL")

employee_df.filter(
col("salary").isNull()
).show()

# ---------------------------------------------------

# 2. Find rows where name is NULL

# ---------------------------------------------------

print("\nROWS WHERE NAME IS NULL")

employee_df.filter(
col("name").isNull()
).show()

# ---------------------------------------------------

# 3. Remove rows where salary is NULL

# ---------------------------------------------------

print("\nREMOVE ROWS WHERE SALARY IS NULL")

salary_not_null_df = employee_df.dropna(
subset=["salary"]
)

salary_not_null_df.show()

# ---------------------------------------------------

# 4. Fill NULL salary with 0

# ---------------------------------------------------

print("\nFILL NULL SALARY WITH 0")

salary_filled_df = employee_df.fillna(
0,
subset=["salary"]
)

salary_filled_df.show()

# ---------------------------------------------------

# 5. Fill NULL name with Unknown

# ---------------------------------------------------

print("\nFILL NULL NAME WITH UNKNOWN")

name_filled_df = employee_df.fillna(
"Unknown",
subset=["name"]
)

name_filled_df.show()

# ---------------------------------------------------

# 6. Fill multiple NULL columns using a dictionary

# ---------------------------------------------------

print("\nFILL MULTIPLE NULL COLUMNS")

filled_df = employee_df.fillna({
"salary": 0,
"name": "Unknown"
})

filled_df.show()

# ---------------------------------------------------

# 7. Remove completely duplicate rows

# ---------------------------------------------------

print("\nREMOVE COMPLETELY DUPLICATE ROWS")

no_duplicates_df = filled_df.dropDuplicates()

no_duplicates_df.show()

# ---------------------------------------------------

# 8. Remove duplicates based on employee_id

# ---------------------------------------------------

print("\nREMOVE DUPLICATES BASED ON EMPLOYEE ID")

unique_employee_df = filled_df.dropDuplicates(
["employee_id"]
)

unique_employee_df.show()

# ---------------------------------------------------

# 9. Convert salary from integer to double

# ---------------------------------------------------

print("\nCONVERT SALARY TO DOUBLE")

salary_double_df = unique_employee_df.withColumn(
"salary",
col("salary").cast("double")
)

salary_double_df.show()

print("\nSCHEMA AFTER CONVERTING SALARY TO DOUBLE")
salary_double_df.printSchema()

# ---------------------------------------------------

# 10. Convert joining_date from string to date

# ---------------------------------------------------

print("\nCONVERT JOINING DATE TO DATE TYPE")

date_converted_df = salary_double_df.withColumn(
"joining_date",
to_date(
col("joining_date"),
"dd-MM-yyyy"
)
)

date_converted_df.show()

print("\nSCHEMA AFTER DATE CONVERSION")
date_converted_df.printSchema()

# ---------------------------------------------------

# Final cleaning pipeline

# ---------------------------------------------------

print("\nFINAL CLEANED DATA")

cleaned_employee_df = (
employee_df
.fillna({
"salary": 0,
"name": "Unknown"
})
.dropDuplicates(["employee_id"])
.withColumn(
"salary",
col("salary").cast("double")
)
.withColumn(
"joining_date",
to_date(
col("joining_date"),
"dd-MM-yyyy"
)
)
)

cleaned_employee_df.show()

print("\nFINAL SCHEMA")
cleaned_employee_df.printSchema()

# Stop SparkSession

spark.stop()
