from pyspark.sql import *
from pyspark import *
import os

"""DO NOT FORGET TO CHANGE YOUR IP OF SPARK MASTER"""
spark = SparkSession.builder. \
    appName("Spark_Standalone_Cluster"). \
    master("spark://192.168.213.1:7077"). \
    config("spark.driver.memory", "4g"). \
    config("spark.driver.cores", "4"). \
    config("spark.executor.memory", "4g"). \
    config("spark.executor.cores", "3"). \
    config("spark.cores.max", "12"). \
    getOrCreate()

#master("local[*]"). \
#master("spark://192.168.1.7:7077"). \
#spark.sparkContext.setLogLevel("ERROR")

path = "D:/00 Project/00 My Project/Dataset/la0730.csv"
df = spark.read.csv(path, header=True).select("name", "Zip code", "Number of reviewers", "Overall score")
df = df.withColumn("Overall score", df["Overall score"].cast('double'))
df.groupBy("Zip code").sum("Overall score").show()

"""
path = "D:/00 Project/01 Knowledge Transfer/Big Data/Dataset/Amazon Reviews/"
df = spark.read.csv(path, sep='\t', header=True)
df = df.withColumn("star_rating", df["star_rating"].cast('double'))
df.groupBy("product_title").avg("star_rating").show()
"""