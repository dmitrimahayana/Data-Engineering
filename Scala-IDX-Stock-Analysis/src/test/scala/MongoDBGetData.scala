import CustomLibrary.SparkConnection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object MongoDBGetData {
  def main(args: Array[String]): Unit = {
    // Create Spark Session
    val sparkMaster = "spark://172.20.224.1:7077"
    val sparkAppName = "Scala REST API IDX Stock Prediction"
    val sparkConn = new SparkConnection(sparkMaster, sparkAppName)
    sparkConn.CreateSparkSession()
    val spark = sparkConn.spark.getOrCreate()
    println("Spark Version: " + spark.version)

    val kafkaStocks = sparkConn.MongoDBGetAllData("mongodb://localhost:27017/kafka.stock-stream");
    var allTicker = kafkaStocks.select("ticker").groupBy("ticker").count().select("ticker")
    val windowSpec  = Window.orderBy("ticker")
    allTicker = allTicker.withColumn("id",row_number.over(windowSpec))
    allTicker.show()

//    val findTicker = "GOTO"
//    var detailTicker = kafkaStocks.filter("ticker == '"+findTicker+"'")
//    detailTicker = detailTicker.withColumn("id", org.apache.spark.sql.functions.concat(col("ticker"), lit("-"), col("date")))
//    detailTicker.show(5)

//    var detailTicker = kafkaStocks.withColumn(
//      "rank", dense_rank().over(Window.partitionBy("ticker").orderBy(desc("date"))))
    var detailTicker = kafkaStocks
//    detailTicker.filter("ticker == 'BBCA'").show(10)
//    detailTicker.filter("ticker == 'BBRI'").show(10)
//    detailTicker.filter("ticker == 'BMRI'").show(10)
    detailTicker = detailTicker.filter("rank == 1")
    detailTicker.show()
  }

}
