# Demonstrates department-wise and company-wide aggregations
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, sum

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("Day 5 - Aggregations")
    .getOrCreate()
)

# Read employee data
df = spark.read.csv(
    "data/employees.csv",
    header=True,
    inferSchema=True
)

print("===== Original Employee Data =====")
df.show()

# Count all employees
print("===== Total Employee Count =====")
print(df.count())

# Count employees in each department
print("===== Employee Count by Department =====")
df.groupBy("department").count().show()

# Total salary by department
print("===== Total Salary by Department =====")
df.groupBy("department").sum("salary").show()

# Average salary by department
print("===== Average Salary by Department =====")
df.groupBy("department").avg("salary").show()

# Highest salary in each department
print("===== Highest Salary by Department =====")
df.groupBy("department").max("salary").show()

# Lowest salary in each department
print("===== Lowest Salary by Department =====")
df.groupBy("department").min("salary").show()

# Highest salary in the entire company
print("===== Highest Salary in Company =====")
df.select(max("salary").alias("highest_salary")).show()

# Lowest salary in the entire company
print("===== Lowest Salary in Company =====")
df.select(min("salary").alias("lowest_salary")).show()

# Total company salary
print("===== Total Company Salary =====")
df.select(sum("salary").alias("total_salary")).show()

# Average company salary
print("===== Average Company Salary =====")
df.select(avg("salary").alias("average_salary")).show()

spark.stop()