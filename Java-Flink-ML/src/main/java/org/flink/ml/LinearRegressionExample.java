package org.flink.ml;

import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.regression.linearregression.LinearRegression;
import org.apache.flink.ml.regression.linearregression.LinearRegressionModel;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/** Simple program that trains a LinearRegression model and uses it for regression. */
public class LinearRegressionExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Generates input data.
        DataStream<Row> inputStream =
                env.fromElements(
                        Row.of(Vectors.dense(2, 1), 4.0),
                        Row.of(Vectors.dense(3, 2), 7.0),
                        Row.of(Vectors.dense(4, 3), 10.0),
                        Row.of(Vectors.dense(2, 4), 10.0),
                        Row.of(Vectors.dense(2, 2), 6.0),
                        Row.of(Vectors.dense(4, 3), 10.0),
                        Row.of(Vectors.dense(1, 2), 5.0),
                        Row.of(Vectors.dense(5, 3), 11.0));

        // Generates output data
        DataStream<Row> testingStream =
                env.fromElements(
                        Row.of(Vectors.dense(6, 1), 4.0),
                        Row.of(Vectors.dense(3, 7), 7.0),
                        Row.of(Vectors.dense(8, 3), 10.0),
                        Row.of(Vectors.dense(2, 9), 10.0),
                        Row.of(Vectors.dense(10, 2), 6.0));

//        Table inputTable = tEnv.fromDataStream(inputStream).as("features", "label", "weight");
//        Table testingTable = tEnv.fromDataStream(testingStream).as("features", "label", "weight");
        Table inputTable = tEnv.fromDataStream(inputStream).as("features", "label");
        Table testTable = tEnv.fromDataStream(testingStream).as("features", "label");

        // Creates a LinearRegression object and initializes its parameters.
//        LinearRegression lr = new LinearRegression().setWeightCol("weight");
        LinearRegression lr = new LinearRegression();

        // Trains the LinearRegression Model.
        LinearRegressionModel lrModel = lr.fit(inputTable);

        // Uses the LinearRegression Model for predictions.
        Table outputTable1 = lrModel.transform(inputTable)[0];

        // Extracts and displays the results.
        for (CloseableIterator<Row> it = outputTable1.execute().collect(); it.hasNext(); ) {
            Row row = it.next();
            DenseVector features = (DenseVector) row.getField(lr.getFeaturesCol());
            double expectedResult = (Double) row.getField(lr.getLabelCol());
            double predictionResult = (Double) row.getField(lr.getPredictionCol());
            System.out.printf(
                    "Features Input: %s \tExpected Result: %s \tPrediction Result: %s\n",
                    features, expectedResult, predictionResult);
        }

        // Uses the LinearRegression Model for predictions.
        Table outputTable2 = lrModel.transform(testTable)[0];

        // Extracts and displays the results.
        for (CloseableIterator<Row> it = outputTable2.execute().collect(); it.hasNext(); ) {
            Row row = it.next();
            DenseVector features = (DenseVector) row.getField(lr.getFeaturesCol());
            double expectedResult = (Double) row.getField(lr.getLabelCol());
            double predictionResult = (Double) row.getField(lr.getPredictionCol());
            System.out.printf(
                    "Features Test: %s \tExpected Result: %s \tPrediction Result: %s\n",
                    features, expectedResult, predictionResult);
        }
    }
}

