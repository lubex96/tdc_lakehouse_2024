from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestApp") \
    .getOrCreate()

data = [("Alice", 29), ("Bob", 31), ("Cathy", 22)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

spark.stop()

