package org.flink.ml;

import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.api.Stage;
import org.apache.flink.ml.builder.Pipeline;
import org.apache.flink.ml.builder.PipelineModel;
import org.apache.flink.ml.feature.featurehasher.FeatureHasher;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.SparseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.regression.linearregression.LinearRegression;
import org.apache.flink.ml.regression.linearregression.LinearRegressionModel;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 * Simple program that trains a LinearRegression model and uses it for regression.
 */
public class LinearRegressionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Generates input data.
        DataStream<Row> inputStream =
                env.fromElements(
                        Row.of("IDX-001", 20, 15, 4.0),
                        Row.of("IDX-002", 21, 10, 6.0),
                        Row.of("IDX-003", 22, 5, 2.0),
                        Row.of("IDX-004", 23, 20, 8.0),
                        Row.of("IDX-005", 24, 35, 3.0));

//        Table trainData = tEnv.fromDataStream(inputStream).as("features", "label", "weight");
        Table trainData = tEnv.fromDataStream(inputStream).as("features", "col1", "col2", "label");

        // Creates a FeatureHasher object and initializes its parameters.
        FeatureHasher featureHash =
                new FeatureHasher()
                        .setInputCols("features", "col1", "col2")
                        .setOutputCol("output")
                        .setNumFeatures(1000);

        // Uses the FeatureHasher object for feature transformations.
        Table outputTable = featureHash.transform(trainData)[0];

        //Print FeatureHasher Result
        for (CloseableIterator<Row> it = outputTable.execute().collect(); it.hasNext(); ) {
            Row row = it.next();
//            System.out.println("Value: " + row.getField("features") + " --- " + row.getField("label"));
            Object[] inputValues = new Object[featureHash.getInputCols().length];
            for (int i = 0; i < featureHash.getInputCols().length; i++) {
                inputValues[i] = row.getField(featureHash.getInputCols()[i]);
            }
            SparseVector outputValue = (SparseVector) row.getField(featureHash.getOutputCol());
            System.out.printf(
                    "Input Values: %s \tOutput Value: %s\n",
                    Arrays.toString(inputValues), outputValue);
        }

        // Creates a LinearRegression object and initializes its parameters.
//        LinearRegression lr = new LinearRegression().setWeightCol("weight");
        LinearRegression lr = new LinearRegression();

        // Trains the LinearRegression Model.
        LinearRegressionModel lrModel = lr.fit(outputTable);

        // Uses the LinearRegression Model for predictions.
        Table outputPrediction = lrModel.transform(outputTable)[0];

//        LinearRegressionModel lrModelA = new LinearRegressionModel().setModelData(trainData);
//        System.out.println(lrModelA);
//        Estimator estimatorA = new LinearRegression();
//        System.out.println(estimatorA);
//
//        List<Stage<?>> stages = Arrays.asList(lrModelA, estimatorA);
//        Pipeline pipeline = new Pipeline(stages);
//        // Fit the model
//        PipelineModel trainedModel = pipeline.fit(trainData);
//        // Predict data
//        Table outputTable1 = trainedModel.transform(predictData)[0];
//        outputTable1.printSchema();

//        // Extracts and displays the results.
//        for (CloseableIterator<Row> it = outputTable1.execute().collect(); it.hasNext(); ) {
//            Row row = it.next();
//            DenseVector features = (DenseVector) row.getField(lr.getFeaturesCol());
//            double expectedResult = (Double) row.getField(lr.getLabelCol());
//            double predictionResult = (Double) row.getField(lr.getPredictionCol());
//            System.out.printf(
//                    "Features Input: %s \tExpected Result: %s \tPrediction Result: %s\n",
//                    features, expectedResult, predictionResult);
//        }

//        // Save the model to a file
//        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("model.ser"))) {
//            oos.writeObject(trainedModel);
//        }
//
//        // Load the model from a file
//        Pipeline loadedModel;
//        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream("model.ser"))) {
//            loadedModel = (Pipeline) ois.readObject();
//        }

        // Execute program, beginning computation.
        env.execute("Flink ML Regression");
    }
}

