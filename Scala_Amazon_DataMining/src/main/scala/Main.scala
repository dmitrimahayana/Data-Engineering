import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.sql.functions._
import scala.collection.Seq
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("AI Amazon Product")
      .master("spark://192.168.1.7:7077")
      .config("spark.driver.memory", "4g")
      .config("spark.driver.cores", "4")
      .config("spark.executor.memory", "4g")
      .config("spark.executor.cores", "3")
      .config("spark.cores.max", "12")
      .config("spark.sql.files.maxPartitionBytes", "12345678")
      .config("spark.sql.files.openCostInBytes", "12345678")
      .config("spark.sql.broadcastTimeout", "1000")
      .config("spark.sql.autoBroadcastJoinThreshold", "100485760")
      .config("spark.sql.shuffle.partitions", "1000")
      .getOrCreate()


    import spark.implicits._ //to support symbol $

    val path_file = "D:/00 Project/01 Knowledge Transfer/Big Data/Dataset/Amazon Reviews/"
    //val filename = "amazon_reviews_us_Books_v1_02.tsv" //for testing purposes
    val filename = "*tsv" //real 25GB data
    val df = spark.read.format("csv") // Use "csv" regardless of TSV or CSV.
      .option("header", "true") // Does the file have a header line?
      .option("delimiter", "\t") // Set delimiter to tab or comma.
      .load(path_file + filename)

    """
    //Filter Data
    val clean_df = df.na.drop("any") //delete any row if there is NULL value

    val df_group_star =
      clean_df.groupBy("product_id", "product_title", "product_category")
        .agg(
          avg("star_rating").as("Average_Rating"),
          sum("total_votes").as("Sum_Votes")
        )
        .orderBy(desc("Average_Rating"))
        .withColumn("Average_Rating", format_number($"Average_Rating", 1))
    df_group_star.show(20)

    """
    //Filter Data
    val clean_df = df.na.drop("any", Seq("product_id", "star_rating")) //delete any row if there is NULL value

    val new_df = clean_df.select("customer_id", "product_id", "star_rating")
      .withColumn("customer_id", $"customer_id" cast "Int")
      .withColumn("product_id", $"product_id" cast "Int")
      .withColumn("star_rating", $"star_rating" cast "Double")

    val new_df2 = new_df.filter("product_id is not null and star_rating is not null")

    val ratings_df = new_df2.map(row => (row.getInt(0), row.getInt(1), row.getDouble(2)) match {
      case (user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    })

    //Split the data into training and test
    val Array(training_data, test_data) = ratings_df.randomSplit(Array(0.8, 0.2), seed = 11L)

    val rank = 1
    val numIterations = 1
    val model = ALS.train(training_data.rdd, rank, numIterations, 0.01)
    new_df2.show()
    // Evaluate the model on rating data
    val usersProducts = test_data.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions =
      model.predict(usersProducts.rdd).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    val ratesAndPreds = test_data.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.rdd.join(predictions)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println(s"Mean Squared Error = $MSE")
    val MSE_df = Seq(("MSE", MSE)).toDF()
    MSE_df.write.format("json").save("C:/Users/ASUS/IdeaProjects/Scala_Amazon_DataMining/out/")

    // Save and load model
    val model_path = "C:/Users/ASUS/IdeaProjects/Scala_Amazon_DataMining/out/"
    model.save(spark.sparkContext, model_path)

  }
}