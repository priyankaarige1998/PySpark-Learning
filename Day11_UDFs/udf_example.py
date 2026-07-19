from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


# ---------------------------------------------------
# Create SparkSession
# ---------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Day 11 - User Defined Functions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ---------------------------------------------------
# Read employee CSV
# ---------------------------------------------------

employee_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/employees.csv")
)

print("\nORIGINAL DATA")
employee_df.show()


# ---------------------------------------------------
# Python Function
# ---------------------------------------------------

def classify_salary(salary):

    if salary >= 80000:
        return "High"

    elif salary >= 60000:
        return "Medium"

    else:
        return "Low"


# ---------------------------------------------------
# Convert Python function into UDF
# ---------------------------------------------------

salary_udf = udf(
    classify_salary,
    StringType()
)


# ---------------------------------------------------
# Apply UDF
# ---------------------------------------------------

employee_df = employee_df.withColumn(
    "salary_category",
    salary_udf(col("salary"))
)

print("\nSALARY CLASSIFICATION")

employee_df.show()


spark.stop()