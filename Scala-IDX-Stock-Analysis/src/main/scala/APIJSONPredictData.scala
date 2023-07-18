import CustomLibrary.SparkConnection
import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.apache.spark.ml.PipelineModel
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn

object APIJSONPredictData {
  // Create Spark Session
  val sparkMaster = "spark://192.168.9.195:7077"
  val sparkAppName = "Scala REST API IDX Stock Prediction"
  val sparkConn = new SparkConnection(sparkMaster, sparkAppName)
  sparkConn.CreateSparkSession()
  println("Spark Version: " + sparkConn.spark.getOrCreate().version)

  // needed to run the route
  implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "SprayExample")
  // needed for the future map/flatmap in the end and future in fetchItem and saveOrder
  implicit val executionContext: ExecutionContext = system.executionContext

  var listStockObj: List[Stock] = Nil

  // domain model
  final case class Stock(date: String, ticker: String, open: Double, volume: Long, prediction: Double)
  final case class ListStock(listStock: List[Stock])

  // formats for unmarshalling and marshalling
  implicit val itemFormat: RootJsonFormat[Stock] = jsonFormat5(Stock.apply)
  implicit val orderFormat: RootJsonFormat[ListStock] = jsonFormat1(ListStock.apply)

  // (fake) async database query api
  def findTicker(ticker: String): Future[Option[Stock]] = Future {
    listStockObj.find(o => o.ticker.toLowerCase() == ticker.toLowerCase())
  }

  def fetchAll(): Future[List[Stock]] = Future {
    listStockObj
  }

  def saveStock(order: ListStock): Future[Done] = {
    var df = sparkConn.spark.getOrCreate().createDataFrame(order.listStock)
    df = df.select("date", "ticker", "open", "volume")
    println(df.printSchema)

    // And load it back in during production
    println("load model...")
    val model2 = PipelineModel.load("D:/00 Project/00 My Project/IdeaProjects/Scala-IDX-Stock-Analysis/modelGaussian")

    // Make predictions.
    println("Testing Data Pipeline...")
    val predictions = model2.transform(df)

    // Select example rows to display.
    predictions.select("date", "ticker", "open", "volume", "prediction").show(20)

    import org.json4s._
    import org.json4s.native.JsonMethods._
    // Return into JSON format
    implicit val formats = DefaultFormats
    val jsonStringArray = predictions.select("date", "ticker", "open", "volume", "prediction").toJSON.collect()
    for (row <- jsonStringArray) {
      val result = parse(row).extract[Stock]
      println(result)
      listStockObj = listStockObj :+ result
    }
    Future {Done}
  }

  def main(args: Array[String]): Unit = {
    val route: Route =
      concat(
        get {
          pathPrefix("ticker" / Segment) { productName =>
            val maybeItem: Future[Option[Stock]] = findTicker(productName)
            onSuccess(maybeItem) {
              case Some(item) => complete(item)
              case None => complete(StatusCodes.NotFound)
            }
          }
        },
        get {
          pathPrefix("list") {
            val maybeItem: Future[List[Stock]] = fetchAll()
            onSuccess(maybeItem) {
              case item if item.nonEmpty => complete(item)
              case _ => complete(StatusCodes.NotFound)
            }
          }
        },
        post {
          path("predict-stock") {
            entity(as[ListStock]) { dummy =>
              val saved: Future[Done] = saveStock(dummy)
              onSuccess(saved) { _ => // we are not interested in the result value `Done` but only in the fact that it was successful
                complete("Stock Predicted")
              }
            }
          }
        }
      )

    // Start Akka API Server
    val bindingFuture = Http().newServerAt("localhost", 9090).bind(route)
    println(s"Server online at http://localhost:9090/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done

  }

}
