from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
from pandas import DataFrame

spark = SparkSession\
    .builder \
    .master("spark://192.168.213.1:7077") \
    .appName("FetchDataNOSQL") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost/") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost/") \
    .getOrCreate()

working_directory = 'D:/00 Project/00 My Project/Jars/*'


def pymongo_get_data():
    CONNECTION_STRING = "mongodb://localhost/"
    client = MongoClient(CONNECTION_STRING)
    dbname = client.Dataset
    collection_name = dbname.inventory
    item_details = collection_name.find({"instock":{"$gt":60}})
    items_df = DataFrame(item_details)
    print(items_df)


if __name__ == "__main__":
    df = spark.read.format("mongodb").option("uri", "mongodb://127.0.0.1/Dataset.inventory").load()
