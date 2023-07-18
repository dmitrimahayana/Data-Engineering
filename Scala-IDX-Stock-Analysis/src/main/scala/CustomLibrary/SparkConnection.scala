package CustomLibrary

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

class SparkConnection(var ConnString: String, var AppName: String){
  var spark = SparkSession.builder()

  def CreateSparkSession(): Unit = {
    val localSpark = SparkSession
      .builder()
      .appName(AppName)
      .master(ConnString)
      .config("spark.executor.instances", "2")
      .config("spark.executor.memory", "6g")
      .config("spark.executor.cores", "3")
      .config("spark.driver.memory", "6g")
      .config("spark.driver.cores", "3")
      .config("spark.cores.max", "6")
      .config("spark.sql.files.maxPartitionBytes", "12345678")
      .config("spark.sql.files.openCostInBytes", "12345678")
      .config("spark.sql.broadcastTimeout", "1000")
      .config("spark.sql.autoBroadcastJoinThreshold", "100485760")
      .config("spark.sql.shuffle.partitions", "1000")

    this.spark = localSpark
  }

  def CloseSession(): Unit = {
    this.spark.getOrCreate().close()
  }

  def CreateMongoDBSession(ConnString: String, Database: String, Collection: String): sql.DataFrame = {
    val df = spark.getOrCreate()
      .read
      .format("mongodb")
      .option("database", Database)
      .option("collection", Collection)
      .option("spark.mongodb.input.partitioner", "MongoSinglePartitioner")
      .option("connection.uri", ConnString)
      .load()

    df.printSchema()
    df
  }

}
