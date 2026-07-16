from pyspark.sql import SparkSession

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("Day 6 - Joins and Spark SQL")
    .getOrCreate()
)

# Read Employees CSV
employees = spark.read.csv(
    "data/employees.csv",
    header=True,
    inferSchema=True
)

# Read Departments CSV
departments = spark.read.csv(
    "data/departments.csv",
    header=True,
    inferSchema=True
)

print("===== Employees Data =====")
employees.show()

print("===== Departments Data =====")
departments.show()

# Inner Join
print("===== Inner Join =====")
inner_join = employees.join(
    departments,
    on="dept_id",
    how="inner"
)
inner_join.show()

# Left Join
print("===== Left Join =====")
left_join = employees.join(
    departments,
    on="dept_id",
    how="left"
)
left_join.show()

# Right Join
print("===== Right Join =====")
right_join = employees.join(
    departments,
    on="dept_id",
    how="right"
)
right_join.show()

# Full Join
print("===== Full Join =====")
full_join = employees.join(
    departments,
    on="dept_id",
    how="full"
)
full_join.show()

# Register DataFrames as temporary SQL views
employees.createOrReplaceTempView("employees")
departments.createOrReplaceTempView("departments")

# SQL: Display all employees
print("===== Spark SQL: All Employees =====")
spark.sql("""
SELECT *
FROM employees
""").show()

# SQL: Employees with salary greater than 70000
print("===== Spark SQL: Salary Greater Than 70000 =====")
spark.sql("""
SELECT emp_id, name, salary
FROM employees
WHERE salary > 70000
ORDER BY salary DESC
""").show()

# SQL Inner Join
print("===== Spark SQL: Inner Join =====")
spark.sql("""
SELECT
    e.emp_id,
    e.name,
    e.salary,
    d.department
FROM employees e
INNER JOIN departments d
ON e.dept_id = d.dept_id
""").show()

# SQL Left Join
print("===== Spark SQL: Left Join =====")
spark.sql("""
SELECT
    e.emp_id,
    e.name,
    e.salary,
    d.department
FROM employees e
LEFT JOIN departments d
ON e.dept_id = d.dept_id
""").show()

# SQL average salary by department
print("===== Spark SQL: Average Salary by Department =====")
spark.sql("""
SELECT
    d.department,
    AVG(e.salary) AS average_salary
FROM employees e
INNER JOIN departments d
ON e.dept_id = d.dept_id
GROUP BY d.department
ORDER BY average_salary DESC
""").show()

spark.stop()