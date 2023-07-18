//package io.id.stock.analysis;
//
//import org.apache.spark.ml.Pipeline;
//import org.apache.spark.ml.PipelineModel;
//import org.apache.spark.ml.PipelineStage;
//import org.apache.spark.ml.classification.LogisticRegression;
//import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
//import org.apache.spark.ml.feature.*;
//import org.apache.spark.sql.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import com.mongodb.spark.sql.connector.read.MongoInputPartition;
//
//import java.util.concurrent.TimeoutException;
//
//public class StocksPrediction {
//
//    private static final Logger log = LoggerFactory.getLogger(StocksPrediction.class.getSimpleName());
//
//    public static void main(String[] args) throws TimeoutException, InterruptedException {
//        String sparkMaster = "spark://192.168.1.4:7077";
//
//        // Create SparkSession
//        SparkSession spark = SparkSession.builder()
//                .appName("StocksPredictionAppJAVA")
//                .master(sparkMaster)
//                .config("spark.driver.memory", "2g")
//                .config("spark.driver.cores", "4")
//                .config("spark.executor.memory", "2g")
//                .config("spark.executor.cores", "4")
//                .config("spark.cores.max", "12")
//                .config("spark.jars", "mongo-spark-connector_2.12-3.0.1-assembly.jar")
//                .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/kafka.stock-stream")
//                .getOrCreate();
//
//        //Add shutdown hook
//        Runtime.getRuntime().addShutdownHook(new Thread(spark::close));
//
//        log.info("****************************************************************************************************");
//
//        Dataset<Row> df = spark
//                .read()
//                .format("mongodb")
//                .option("database", "kafka")
//                .option("collection", "stock-stream")
//                .option("spark.mongodb.input.partitioner", "MongoSinglePartitioner")
//                .option("connection.uri", "mongodb://localhost:27017/kafka.stock-stream")
//                .load();
//
//        df.printSchema();
//
//        try {
////            Dataset<Row> newDf = df.toDF().selectExpr("cast(date as string) date",
////                    "cast(ticker as string) ticker",
////                    "cast(open as double) open",
////                    "cast(high as double) high",
////                    "cast(low as double) low",
////                    "cast(close as double) close",
////                    "cast(volume as long) volume");
////            newDf.printSchema();
//            df = df.select("ticker", "open", "volume", "close");
//            df.printSchema();
//
//            // Split the data into training and test sets (30% held out for testing).
//            Dataset<Row>[] splits = df.randomSplit(new double[]{0.7, 0.3});
//            Dataset<Row> train = splits[0];
//            Dataset<Row> test = splits[1];
//
//            // Define the feature transformation stages
//            StringIndexer tickerIndexer = new StringIndexer()
//                    .setInputCol("ticker")
//                    .setOutputCol("tickerIndex");
//
//            OneHotEncoder tickerOneHotEncoder = new OneHotEncoder()
//                    .setInputCol("tickerIndex")
//                    .setOutputCol("tickerOneHot");
//
//            String[] features = {"tickerOneHot", "open", "volume"};
//            String label = "close";
//
//            VectorAssembler vectorAssembler = new VectorAssembler()
//                    .setInputCols(features)
//                    .setOutputCol("features");
//
//            StandardScaler scaler = new StandardScaler()
//                    .setInputCol("features")
//                    .setOutputCol("scaledFeatures");
//
//            LogisticRegression logisticRegression = new LogisticRegression()
//                    .setFeaturesCol("scaledFeatures")
//                    .setLabelCol(label);
//
//            // Assemble the pipeline
//            PipelineStage[] stages = {tickerIndexer, tickerOneHotEncoder, vectorAssembler, scaler, logisticRegression};
//            Pipeline pipeline = new Pipeline().setStages(stages);
//
//            // Fit the pipeline
//            PipelineModel model = pipeline.fit(train);
////
////            // Predicting Result for the test set
////            Dataset<Row> results = model.transform(test);
////
////            // Evaluating the Result
////            BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
////                    .setLabelCol(label)
////                    .setRawPredictionCol("rawPrediction")
////                    .setMetricName("areaUnderROC");
////
////            double accuracy = evaluator.evaluate(results);
////            System.out.println("Accuracy: "+accuracy);
//        } catch (Exception e){
//            log.info("ERROR SPARK DF: "+e.getMessage());
//        }
//
////        String path = "D:/00 Project/00 My Project/Dataset/la0730.csv";
////        try {
////            Dataset<Row> df = spark.read().option("header", "true").csv(path).select("name", "Zip code", "Number of reviewers", "Overall score");
////
////            df = df.withColumn("Overall score", df.col("Overall score").cast("Double"));
////            Dataset<Row> new_df = df.groupBy("Zip code").sum("Overall score");
////            new_df.show(5);
////
////            df.write().format("mongodb").mode("overwrite").save();
////        } catch (Exception e){
////            log.info("ERROR SPARK DF: "+e.getMessage());
////        }
//
//        log.info("****************************************************************************************************");
//        spark.close();
//    }
//}
