from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder \
    .appName("Day 4 - Data Transformations") \
    .getOrCreate()

# Read CSV file
df = spark.read.csv(
    "data/employees.csv",
    header=True,
    inferSchema=True
)

print("===== Original Data =====")
df.show()

# Select specific columns
print("===== Select Name and Salary =====")
df.select("name", "salary").show()

# Filter employees with salary > 70000
print("===== Salary Greater Than 70000 =====")
df.filter(df.salary > 70000).show()

# Filter IT department
print("===== IT Department =====")
df.where(df.department == "IT").show()

# Add a new column
print("===== Bonus Salary =====")
df.withColumn(
    "bonus_salary",
    col("salary") + 5000
).show()

# Remove a column
print("===== Drop Department =====")
df.drop("department").show()

# Sort salary (Ascending)
print("===== Salary Ascending =====")
df.sort("salary").show()

# Sort salary (Descending)
print("===== Salary Descending =====")
df.sort(col("salary").desc()).show()

# Display unique departments
print("===== Distinct Departments =====")
df.select("department").distinct().show()

spark.stop()