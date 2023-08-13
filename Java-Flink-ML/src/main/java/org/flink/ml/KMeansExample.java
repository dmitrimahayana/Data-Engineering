/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.flink.ml;

import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class KMeansExample {

	private static final Logger log = LoggerFactory.getLogger(KMeansExample.class.getSimpleName());

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		String featuresCol = "features";
		String predictionCol = "prediction";

		// Generate train data and predict data as DataStream.
		DataStream<DenseVector> inputStream = env.fromElements(
				Vectors.dense(0.0, 0.0),
				Vectors.dense(0.0, 0.3),
				Vectors.dense(0.3, 0.0),
				Vectors.dense(9.0, 0.0),
				Vectors.dense(9.0, 0.6),
				Vectors.dense(9.6, 0.0)
		);

		// Convert data from DataStream to Table, as Flink ML uses Table API.
		Table input = tEnv.fromDataStream(inputStream).as(featuresCol);

		// Creates a K-means object and initialize its parameters.
		KMeans kmeans = new KMeans()
				.setK(2)
				.setSeed(1L)
				.setFeaturesCol(featuresCol)
				.setPredictionCol(predictionCol);

		// Trains the K-means Model.
		KMeansModel model = kmeans.fit(input);

		// Use the K-means Model for predictions.
		Table output = model.transform(input)[0];

		// Extracts and displays prediction result.
		for (CloseableIterator<Row> it = output.execute().collect(); it.hasNext(); ) {
			Row row = it.next();
			DenseVector vector = (DenseVector) row.getField(featuresCol);
			int clusterId = (Integer) row.getField(predictionCol);
			log.info("Vector: " + vector + "\tCluster ID: " + clusterId);
		}

		// Execute program, beginning computation.
		env.execute("Flink ML Sample");
	}
}
