import CustomLibrary.SparkConnection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

object MongoDBGetData {
  def main(args: Array[String]): Unit = {
    // Create Spark Session
    val sparkMaster = "spark://172.20.224.1:7077"
    val sparkAppName = "Scala REST API IDX Stock Prediction"
    val sparkConn = new SparkConnection(sparkMaster, sparkAppName)
    sparkConn.CreateSparkSession()
    val spark = sparkConn.spark.getOrCreate()
    println("Spark Version: " + spark.version)

    val kafkaStocks = sparkConn.MongoDBGetAllStock("mongodb://localhost:27017/kafka.stock-stream");
//    var allTicker = kafkaStocks.select("ticker").groupBy("ticker").count().select("ticker")
//    val windowSpec  = Window.orderBy("ticker")
//    allTicker = allTicker.withColumn("id",row_number.over(windowSpec))
//    allTicker.show()

    var allTicker = kafkaStocks
    allTicker = allTicker.withColumn("id", org.apache.spark.sql.functions.concat(col("ticker"), lit("-"), col("date")))
    val allTickerRank1 = allTicker.filter("rank == 1")
    var allTickerRank2 = allTicker.filter("rank == 2")
    allTickerRank2 = allTickerRank2
      .withColumn("ticker2", col("ticker").cast(StringType))
      .withColumn("rank2", col("rank").cast(IntegerType))
      .withColumn("yesterdayclose", col("close").cast(DoubleType))
    allTickerRank2 = allTickerRank2.select("ticker2", "rank2", "yesterdayclose")
    var joinTable = allTickerRank1.join(allTickerRank2, allTickerRank1("ticker") === allTickerRank2("ticker2"), "inner")
    joinTable = joinTable
      .withColumn("status",
        when(col("close") > col("yesterdayclose"), "Up")
          .when (col("close") < col("yesterdayclose"), "Down")
          .otherwise("Stay"))
      .withColumn("change", round((col("close") - col("yesterdayclose"))))
      .withColumn("changeval",
        when(col("change") > lit(0), org.apache.spark.sql.functions.concat(lit("+"), col("change").cast(IntegerType)))
          .when(col("change") < lit(0),col("change").cast(IntegerType))
          .otherwise("0"))
      .withColumn("changepercent",
        when(col("change") > lit(0), org.apache.spark.sql.functions.concat(lit("+"), abs((col("change") / col("yesterdayclose")) * 100).cast(IntegerType), lit("%")))
          .when(col("change") < lit(0), org.apache.spark.sql.functions.concat(lit("-"), abs((col("change") / col("yesterdayclose")) * 100).cast(IntegerType), lit("%")))
          .otherwise("0%"))
    joinTable.select("id","date", "ticker", "open", "volume", "close", "yesterdayclose", "change", "changeval", "changepercent", "status").show()
    joinTable.printSchema()
  }

}
