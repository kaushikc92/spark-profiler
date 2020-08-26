from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("profiler").getOrCreate()
sc = spark.sparkContext
