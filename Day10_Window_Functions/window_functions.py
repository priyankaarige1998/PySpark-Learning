from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    row_number,
    rank,
    dense_rank
)


# ---------------------------------------------------
# Create SparkSession
# ---------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Day 10 - Window Functions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ---------------------------------------------------
# Read employee CSV file
# ---------------------------------------------------

employee_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/employees.csv")
)


print("\nORIGINAL EMPLOYEE DATA")

employee_df.show()


print("\nEMPLOYEE SCHEMA")

employee_df.printSchema()


# ---------------------------------------------------
# Create window specification
# ---------------------------------------------------

department_salary_window = (
    Window
    .partitionBy("department")
    .orderBy(col("salary").desc())
)


# ---------------------------------------------------
# 1. Apply row_number()
# ---------------------------------------------------

print("\nROW NUMBER BASED ON SALARY")

row_number_df = employee_df.withColumn(
    "row_number",
    row_number().over(department_salary_window)
)

row_number_df.show()


# ---------------------------------------------------
# 2. Apply rank()
# ---------------------------------------------------

print("\nRANK BASED ON SALARY")

rank_df = employee_df.withColumn(
    "salary_rank",
    rank().over(department_salary_window)
)

rank_df.show()


# ---------------------------------------------------
# 3. Apply dense_rank()
# ---------------------------------------------------

print("\nDENSE RANK BASED ON SALARY")

dense_rank_df = employee_df.withColumn(
    "salary_dense_rank",
    dense_rank().over(department_salary_window)
)

dense_rank_df.show()


# ---------------------------------------------------
# 4. Display all three ranking methods together
# ---------------------------------------------------

print("\nCOMPARISON OF RANKING FUNCTIONS")

ranking_comparison_df = (
    employee_df
    .withColumn(
        "row_number",
        row_number().over(department_salary_window)
    )
    .withColumn(
        "rank",
        rank().over(department_salary_window)
    )
    .withColumn(
        "dense_rank",
        dense_rank().over(department_salary_window)
    )
)

ranking_comparison_df.show()


# ---------------------------------------------------
# 5. Highest-paid employee in each department
# ---------------------------------------------------

print("\nHIGHEST-PAID EMPLOYEE IN EACH DEPARTMENT")

highest_paid_df = (
    employee_df
    .withColumn(
        "row_number",
        row_number().over(department_salary_window)
    )
    .filter(col("row_number") == 1)
    .drop("row_number")
)

highest_paid_df.show()


# ---------------------------------------------------
# 6. Rank all employees based on salary
#    without separating departments
# ---------------------------------------------------

overall_salary_window = (
    Window
    .orderBy(col("salary").desc())
)

print("\nOVERALL EMPLOYEE SALARY RANK")

overall_rank_df = employee_df.withColumn(
    "overall_salary_rank",
    rank().over(overall_salary_window)
)

overall_rank_df.show()


# ---------------------------------------------------
# 7. Top 2 employees from every department
# ---------------------------------------------------

print("\nTOP 2 HIGHEST-PAID EMPLOYEES FROM EACH DEPARTMENT")

top_two_df = (
    employee_df
    .withColumn(
        "row_number",
        row_number().over(department_salary_window)
    )
    .filter(col("row_number") <= 2)
)

top_two_df.show()


# ---------------------------------------------------
# 8. Top 2 salary levels from every department
#    using dense_rank()
# ---------------------------------------------------

print("\nTOP 2 SALARY LEVELS FROM EACH DEPARTMENT")

top_two_salary_levels_df = (
    employee_df
    .withColumn(
        "salary_rank",
        dense_rank().over(department_salary_window)
    )
    .filter(col("salary_rank") <= 2)
)

top_two_salary_levels_df.show()


# ---------------------------------------------------
# Stop SparkSession
# ---------------------------------------------------

spark.stop()