package io.id.stock.analysis;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Collections;

import static org.apache.spark.sql.functions.col;

public class StocksPrediction {

    public static void main(String[] args) {
        // Spark Host
//        String sparkMaster = "spark://192.168.1.4:7077";
        String sparkMaster = "local[*]";

        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("StocksPredictionAppJAVA")
                .master(sparkMaster)
                .config("spark.driver.memory", "1g")
                .config("spark.driver.cores", "2")
//                .config("spark.executor.instances", "2")
                .config("spark.executor.memory", "1g")
                .config("spark.executor.cores", "2")
//                .config("spark.cores.max", "6")
                .getOrCreate();

//        String filePath = "KStream-IDXStock.json";
//        Dataset<Row> df = spark.read().format("json") // Use "csv" regardless of TSV or CSV.
//                .option("header", "true") // Does the file have a header line?
//                .load(filePath);
        // Set up MongoDB connection parameters
        String connectionString = "mongodb://localhost:27017"; // MongoDB server URI
        String databaseName = "kafka"; // Name of the database
        String collectionName = "stock-stream"; // Name of the collection

        // Connect to MongoDB server
        ConnectionString connString = new ConnectionString(connectionString);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .build();

        try {
            // Establish MongoDB connection
            com.mongodb.client.MongoClient mongoClient = MongoClients.create(settings);

            // Access the database
            MongoDatabase database = mongoClient.getDatabase(databaseName);

            // Access the collection
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // Query the collection
            // Define the filter conditions
            Document query = new Document();
            query.append("date", new Document("$regex", ".*2023.*"));
//            query.append("ticker", "BBCA");
            FindIterable<Document> documents = collection.find(query);

            JSONArray jArray = new JSONArray();

            for (Document document : documents) {
                JSONObject jObject = new JSONObject(document.toJson());
                jArray.put(jObject);
            }

            Dataset<Row> df = spark.read().json(spark.createDataset(Collections.singletonList(jArray.toString()), Encoders.STRING()));
//            df = df.filter(col("ticker").equalTo("BBCA")).filter(col("date").contains("2022"));
            df = df.select("ticker", "date", "open", "volume", "close");
            df.printSchema();
            df.show(5);
            System.out.println("Total Data Count: "+df.count());

            // Split the data into training and test sets (30% held out for testing).
            Dataset<Row>[] splits = df.randomSplit(new double[]{0.7, 0.3});
            Dataset<Row> train = splits[0];
            Dataset<Row> test = splits[1];

            // Define the feature transformation stages
            StringIndexer tickerIndexer = new StringIndexer()
                    .setInputCol("ticker")
                    .setInputCol("open")
                    .setOutputCol("tickerIndex");

            OneHotEncoder tickerOneHotEncoder = new OneHotEncoder()
                    .setInputCol("tickerIndex")
                    .setOutputCol("tickerOneHot");

            //Spark cannot use label or feature using double data type
            String[] features = {"tickerOneHot", "open", "close"};
            String label = "volume";

            VectorAssembler vectorAssembler = new VectorAssembler()
                    .setInputCols(features)
                    .setOutputCol("features");

            StandardScaler scaler = new StandardScaler()
                    .setInputCol("features")
                    .setOutputCol("scaledFeatures");

            LogisticRegression logisticRegression = new LogisticRegression()
                    .setFeaturesCol("scaledFeatures")
                    .setLabelCol(label);

            // Assemble the pipeline
            PipelineStage[] stages = {tickerIndexer, tickerOneHotEncoder, vectorAssembler, scaler, logisticRegression};
            Pipeline pipeline = new Pipeline().setStages(stages);

            // Fit the pipeline
            System.out.println("Start Training...");
            PipelineModel model = pipeline.fit(train);

            // Predicting Result for the test set
            System.out.println("Start Testing...");
            Dataset<Row> results = model.transform(test);

            // Evaluating the Result
            System.out.println("Start Evaluating...");
            BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                    .setLabelCol(label)
                    .setRawPredictionCol("rawPrediction")
                    .setMetricName("areaUnderROC");

            double accuracy = evaluator.evaluate(results);
            System.out.println("Accuracy: "+accuracy);
        } catch (Exception e){
            System.out.println("ERROR SPARK: "+e.getMessage());
        }
    }
}
