from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("SparkIntroduction") \
        .getOrCreate()

print("Spark Started Successfully")

spark.stop()
