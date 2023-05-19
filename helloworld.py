from pyspark.sql import SparkSession

SPARK_LOG_LEVELS = {

    # ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
}

print("Hello, PySpark!")
spark = SparkSession.builder.appName("Hello PySpark") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "10g").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

df = spark.read.csv("Online Retail.csv", header=True, escape="\"")
# df = spark.read.csv("test.csv", header=True, escape="\"")
df.show(5,0)
df.count()


