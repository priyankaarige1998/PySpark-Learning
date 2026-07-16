from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("Employee ETL Project")
    .getOrCreate()
)

# Read CSV files
employees = spark.read.csv(
    "data/employees.csv",
    header=True,
    inferSchema=True
)

departments = spark.read.csv(
    "data/departments.csv",
    header=True,
    inferSchema=True
)

print("===== Employees =====")
employees.show()

print("===== Departments =====")
departments.show()

# Remove rows with missing salary
employees = employees.na.drop(subset=["salary"])

print("===== Cleaned Employee Data =====")
employees.show()

# Join Employees and Departments
employee_data = employees.join(
    departments,
    on="dept_id",
    how="inner"
)

print("===== Joined Data =====")
employee_data.show()

# Employees with salary greater than 70000
high_salary = employee_data.filter(
    employee_data.salary > 70000
)

print("===== High Salary Employees =====")
high_salary.show()

# Average salary by department
print("===== Average Salary by Department =====")
employee_data.groupBy("department") \
             .avg("salary") \
             .show()

# Highest salary in the company
print("===== Highest Salary =====")
employee_data.select(
    max("salary").alias("highest_salary")
).show()

# Save output
high_salary.write.mode("overwrite").csv(
    "output/high_salary_employees",
    header=True
)

print("Output saved successfully.")

spark.stop()