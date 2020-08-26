from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("profiler").getOrCreate()
sc = spark.sparkContext
with open('/opt/spark/data/lake/courses/reed.csv', 'r') as f:
    data = f.read()
df = spark.read.csv(sc.parallelize(data.splitlines()))
df.show()
