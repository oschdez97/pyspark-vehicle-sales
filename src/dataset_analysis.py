from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("MyPySparkApp").getOrCreate()
