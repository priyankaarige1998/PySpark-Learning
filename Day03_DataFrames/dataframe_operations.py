from pyspark.sql import SparkSession

#Create Spark Session
spark = SparkSession.builder \
      .appName("Day3 - DataFrames") \
      .getOrCreate()

#Read a CSV File
df = spark.read.csv(
    "data/employees.csv",
    header=True,
    inferSchema=True
)

    


print("=====Employees Data=====")
df.show()

print("=====Schema=====")
df.printSchema()

print("=====No of Rows=====")
print(df.count())

print("=====Statistics=====")
df.describe().show()

print("=====Column Names=====")
print(df.columns)

spark.stop()