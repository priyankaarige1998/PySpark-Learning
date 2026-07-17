import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)


def get_folder_size(folder_path):
    """Return the total size of all files in a folder."""

    total_size = 0

    for root, directories, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            total_size += os.path.getsize(file_path)

    return total_size


spark = (
    SparkSession.builder
    .appName("Day 8 File Formats")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("city", StringType(), True)
])


# 1. Read JSON data
employee_df = (
    spark.read
    .schema(employee_schema)
    .json("data/employees.json")
)


print("JSON DATA")
employee_df.show()


print("JSON SCHEMA")
employee_df.printSchema()


# 2. Write the data as CSV
employee_df.write.mode("overwrite").option(
    "header", "true"
).csv("output/employees_csv")


# 3. Write the data as Parquet
employee_df.write.mode("overwrite").parquet(
    "output/employees_parquet"
)


# 4. Read the Parquet data
parquet_df = spark.read.parquet(
    "output/employees_parquet"
)


print("PARQUET DATA")
parquet_df.show()


print("PARQUET SCHEMA")
parquet_df.printSchema()


# 5. Compare file sizes
csv_size = get_folder_size(
    "output/employees_csv"
)

parquet_size = get_folder_size(
    "output/employees_parquet"
)


print(
    "CSV size:",
    round(csv_size / 1024, 2),
    "KB"
)

print(
    "Parquet size:",
    round(parquet_size / 1024, 2),
    "KB"
)


# 6. Write partitioned Parquet data
employee_df.write.mode("overwrite").partitionBy(
    "department"
).parquet("output/employees_partitioned")


# 7. Read partitioned Parquet data
partitioned_df = spark.read.parquet(
    "output/employees_partitioned"
)


print("PARTITIONED DATA")
partitioned_df.show()


# 8. Filter one department
engineering_df = partitioned_df.filter(
    partitioned_df.department == "Engineering"
)


print("ENGINEERING EMPLOYEES")
engineering_df.show()


spark.stop()