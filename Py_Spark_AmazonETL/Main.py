from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark import *
import os

spark = SparkSession.builder. \
    appName("Spark_Standalone_Cluster"). \
    master("spark://192.168.1.7:7077"). \
    config("spark.driver.memory", "4g"). \
    config("spark.driver.cores", "4"). \
    config("spark.executor.memory", "4g"). \
    config("spark.executor.cores", "3"). \
    config("spark.cores.max", "12"). \
    getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

"""spark://192.168.1.7:7077"""

"""
path = "la0730.csv"
df = spark.read.csv(path, header=True).select("name", "Zip code", "Number of reviewers", "Overall score")
df = df.withColumn("Overall score", df["Overall score"].cast('double'))
df.groupBy("Zip code").sum("Overall score").show()
"""

path = "D:/00 Project/01 Knowledge Transfer/Big Data/Dataset/Amazon Reviews/"
df = spark.read.csv(path, sep='\t', header=True)

#df.show(5)

df = df.withColumn("star_rating", df["star_rating"].cast('double'))
df.groupBy("product_title").avg("star_rating").show()