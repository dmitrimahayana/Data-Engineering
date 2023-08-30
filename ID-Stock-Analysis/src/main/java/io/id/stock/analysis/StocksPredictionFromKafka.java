//package io.id.stock.analysis;
//
//import static org.apache.spark.sql.functions.col;
//
//import org.apache.spark.ml.Pipeline;
//import org.apache.spark.ml.PipelineModel;
//import org.apache.spark.ml.PipelineStage;
//import org.apache.spark.ml.classification.LogisticRegression;
//import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
//import org.apache.spark.ml.feature.OneHotEncoder;
//import org.apache.spark.ml.feature.StandardScaler;
//import org.apache.spark.ml.feature.StringIndexer;
//import org.apache.spark.ml.feature.VectorAssembler;
//import org.apache.spark.sql.*;
//import org.apache.spark.sql.streaming.DataStreamWriter;
//import org.apache.spark.sql.streaming.StreamingQuery;
//import org.apache.spark.sql.streaming.Trigger;
//import org.apache.spark.sql.types.DataType;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//
//import java.util.Arrays;
//import java.util.concurrent.TimeoutException;
//
//public class StockPredictionsFromKafka {
//    private static final Logger log = LoggerFactory.getLogger(StocksPrediction.class.getSimpleName());
//
//    public static void main(String[] args) throws TimeoutException, InterruptedException {
//        String sparkMaster = "spark://192.168.1.4:7077";
//        // Create SparkSession
//        SparkSession spark = SparkSession.builder()
//                .appName("StocksPredictionAppJAVA")
//                .master(sparkMaster)
//                .config("spark.driver.memory", "2g")
//                .config("spark.driver.cores", "4")
//                .config("spark.executor.memory", "2g")
//                .config("spark.executor.cores", "4")
//                .config("spark.cores.max", "12")
//                .getOrCreate();
//
//        Dataset<Row> df = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "192.168.207.8:9092")
//                .option("subscribe", "group.stock")
//                .option("startingOffsets", "earliest")
////                .option("endingOffsets", "latest")
//                .load();
//        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
////        df.printSchema();
//
//        Dataset<Row> df2 = df.withColumn("value",col("value").cast(DataTypes.StringType));
////        df2.printSchema();
//
////        //Add shutdown hook
////        Runtime.getRuntime().addShutdownHook(new Thread(spark::close));
//
//        log.info("****************************************************************************************************");
//        try{
//
//            StructType schema = new StructType()
//                    .add(new StructField("id", DataTypes.StringType, true, null))
//                    .add(new StructField("ticker", DataTypes.StringType, true, null))
//                    .add(new StructField("date", DataTypes.DoubleType, true, null))
//                    .add(new StructField("open", DataTypes.DoubleType, true, null))
//                    .add(new StructField("high", DataTypes.DoubleType, true, null))
//                    .add(new StructField("low", DataTypes.DoubleType, true, null))
//                    .add(new StructField("close", DataTypes.DoubleType, true, null))
//                    .add(new StructField("volume", DataTypes.IntegerType, true, null));
//
//            // Convert JSON string column to struct type
//            Dataset<Row> df3 = df2.withColumn("value", functions.from_json(df2.col("value"), schema));
//            System.out.println("DF3:");
//            df3.printSchema();
//
//            // Convert to multiple columns
////            Dataset<Row> df4 = df3.select("value.ticker","value.date", "value.open", "value.volume", "value.close");
//            Dataset<Row> df4 = df3.select(df3.col("key"), functions.expr("value.*"));
//            System.out.println("DF4:");
//            df4.printSchema();
//
//            df4 = df4.select("ticker", "open", "volume", "close");
//            df4.printSchema();
//
//            // Split the data into training and test sets (30% held out for testing).
//            Dataset<Row>[] splits = df4.randomSplit(new double[]{0.7, 0.3});
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
//
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
////            df4 = df4.filter(col("ticker").equalTo("BBCA")).filter(col("date").equalTo("2023-02-03"));
////            // Query 1: Write to console
////            DataStreamWriter<Row> dataStreamWriter = df4.writeStream()
////                    .format("console")
//////                    .trigger(Trigger.Continuous("10 second"))
////                    .outputMode("append");
////
////            dataStreamWriter.start().awaitTermination();
//        } catch (Exception e){
//            log.info("ERROR SPARK: "+e.getMessage());
//        }
//        log.info("****************************************************************************************************");
//    }
//}
