from enum import Enum
from pyspark.sql import SparkSession

class LOG_LEVELS(Enum):
    ALL   = 'ALL'
    DEBUG = 'DEBUG'
    ERROR = 'ERROR'
    FATAL = 'FATAL'
    INFO  = 'INFO'
    OFF   = 'OFF'
    TRACE = 'TRACE'
    WARN  = 'WARN'

print("Hello, PySpark!")

spark = SparkSession.builder.appName("Hello PySpark") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "10g").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel(LOG_LEVELS.ERROR)

df = spark.read.csv("Online Retail.csv", header=True, escape="\"")
# df = spark.read.csv("test.csv", header=True, escape="\"")
df.show(5,0)
df.count()


