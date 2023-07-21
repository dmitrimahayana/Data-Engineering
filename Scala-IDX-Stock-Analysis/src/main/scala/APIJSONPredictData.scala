import CustomLibrary.SparkConnection
import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn

object APIJSONPredictData {
  // Create Spark Session
  val sparkMaster = "spark://172.20.224.1:7077"
  val sparkAppName = "Scala REST API IDX Stock Prediction"
  val sparkConn = new SparkConnection(sparkMaster, sparkAppName)
  sparkConn.CreateSparkSession()
  val spark = sparkConn.spark.getOrCreate()
  println("Spark Version: " + spark.version)
  // Get Stock from MongoDB
  val kafkaStocks = sparkConn.MongoDBGetAllData("mongodb://localhost:27017/kafka.stock-stream");

//  val conf: Config = ConfigFactory.load("Config/application.conf")
  // needed to run the route
  implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "SprayExample")
  // needed for the future map/flatmap in the end and future in fetchItem and saveOrder
  implicit val executionContext: ExecutionContext = system.executionContext

  var listStockObj: List[Stock] = Nil

  // domain model
  final case class Ticker(id: String, ticker: String)
  final case class Stock(id: String, date: String, ticker: String, open: Double, volume: Long, close: Double)
  final case class StockReqPrediction(date: String, ticker: String, open: Double, volume: Long)
  final case class ListStock(listStock: List[Stock])
  final case class ListStockReqPrediction(listStock: List[StockReqPrediction])

  // formats for unmarshalling and marshalling
  implicit val itemFormat1: RootJsonFormat[Ticker] = jsonFormat2(Ticker.apply)
  implicit val itemFormat2: RootJsonFormat[Stock] = jsonFormat6(Stock.apply)
  implicit val itemFormat3: RootJsonFormat[StockReqPrediction] = jsonFormat4(StockReqPrediction.apply)
  implicit val orderFormat1: RootJsonFormat[ListStock] = jsonFormat1(ListStock.apply)
  implicit val orderFormat2: RootJsonFormat[ListStockReqPrediction] = jsonFormat1(ListStockReqPrediction.apply)

  // Return into JSON format
  def GetStockJSON(dataframe: sql.DataFrame): List[Stock] = {
    import org.json4s._
    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats
    var listObj: List[Stock] = Nil
    val jsonStringArray = dataframe.toJSON.collect()
    for (row <- jsonStringArray) {
      val result = parse(row).extract[Stock]
      println(result)
      listObj = listObj :+ result
    }
    listObj
  }

  def GetTickerJSON(dataframe: sql.DataFrame): List[Ticker] = {
    import org.json4s._
    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats
    var listObj: List[Ticker] = Nil
    val jsonStringArray = dataframe.toJSON.collect()
    for (row <- jsonStringArray) {
      val result = parse(row).extract[Ticker]
      println(result)
      listObj = listObj :+ result
    }
    listObj
  }

  // (fake) async database query api
  def findTickerHistory(ticker: String): Future[List[Stock]] = Future {
    var detailTicker = kafkaStocks.filter("ticker == '" + ticker + "'")
    detailTicker = detailTicker.withColumn("id", org.apache.spark.sql.functions.concat(col("ticker"), lit("-"), col("date")))
    GetStockJSON(detailTicker.select("id","date", "ticker", "open", "volume", "close"))
  }

  def AllTickerLastStock(): Future[List[Stock]] = Future {
    var allTicker = kafkaStocks
    allTicker = allTicker.filter("rank == 1")
    allTicker = allTicker.withColumn("id", org.apache.spark.sql.functions.concat(col("ticker"), lit("-"), col("date")))
    GetStockJSON(allTicker.select("id", "date", "ticker", "open", "volume", "close"))
  }

  def findAllEmitent(): Future[List[Ticker]] = Future {
    var allTicker = kafkaStocks.select("ticker").groupBy("ticker").count().select("ticker")
    val windowSpec = Window.orderBy("ticker")
    allTicker = allTicker.withColumn("id", row_number.over(windowSpec))
    GetTickerJSON(allTicker)
  }

  def predictStock(order: ListStockReqPrediction): Future[Done] = {
    var df = spark.createDataFrame(order.listStock)
    df = df.select("date", "ticker", "open", "volume")
    println(df.printSchema)

    // And load it back in during production
    println("load model...")
    val model2 = PipelineModel.load("D:/00 Project/00 My Project/IdeaProjects/Scala-IDX-Stock-Analysis/modelGaussian")

    // Make predictions.
    println("Testing Data Pipeline...")
    var predictions = model2.transform(df)

    // Select example rows to display.
    predictions = predictions.withColumn("close", col("prediction"))
    predictions = predictions.withColumn("id", org.apache.spark.sql.functions.concat(col("ticker"), lit("-"), col("date")))
    predictions.select("id","date", "ticker", "open", "volume", "prediction", "close").show(20)

    // Return JSON
    val listObj = GetStockJSON(predictions.select("id","date", "ticker", "open", "volume", "close"))
    listStockObj = listObj
    Future {Done}
  }

  def findAllPrediction(): Future[List[Stock]] = Future {
    listStockObj
  }

  def main(args: Array[String]): Unit = {
    val route: Route = cors() {
      concat(
        get {
          pathPrefix("ticker-list-last-stock") {
            val maybeItem: Future[List[Stock]] = AllTickerLastStock()
            onSuccess(maybeItem) {
              case item if item.nonEmpty => complete(item)
              case _ => complete(StatusCodes.NotFound)
            }
          }
        },
        get {
          pathPrefix("ticker" / Segment) { productName =>
            val maybeItem: Future[List[Stock]] = findTickerHistory(productName)
            onSuccess(maybeItem) {
              case item if item.nonEmpty => complete(item)
              case _ => complete(StatusCodes.NotFound)
            }
          }
        },
        get {
          pathPrefix("emitent-list") {
            val maybeItem: Future[List[Ticker]] = findAllEmitent()
            onSuccess(maybeItem) {
              case item if item.nonEmpty => complete(item)
              case _ => complete(StatusCodes.NotFound)
            }
          }
        },
        get {
          pathPrefix("predict-list") {
            val maybeItem: Future[List[Stock]] = findAllPrediction()
            onSuccess(maybeItem) {
              case item if item.nonEmpty => complete(item)
              case _ => complete(StatusCodes.NotFound)
            }
          }
        },
        post {
          path("predict-stock") {
            entity(as[ListStockReqPrediction]) { dummy =>
              val saved: Future[Done] = predictStock(dummy)
              onSuccess(saved) { _ => // we are not interested in the result value `Done` but only in the fact that it was successful
                val maybeItem: Future[List[Stock]] = findAllPrediction()
                onSuccess(maybeItem) {
                  case item if item.nonEmpty => complete(item)
                  case _ => complete(StatusCodes.NotFound)
                }
              }
            }
          }
        }
      )
    }

    // Start Akka API Server
    val bindingFuture = Http().newServerAt("localhost", 9090).bind(route)
    println(s"Server online at http://localhost:9090/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done

  }

}
