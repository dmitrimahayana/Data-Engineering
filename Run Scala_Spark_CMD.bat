spark-submit --master spark://192.168.1.8:7077 --class Main "D:\00 Project\00 My Project\Jars\Scala_Amazon_DataMining_jar\Amazon_ETL.jar"

spark-submit --master spark://192.168.1.8:7077 --class SparkScalaApp "D:\00 Project\00 My Project\IdeaProjects\SparkScalaApp\out\artifacts\SparkScalaApp_jar\SparkScalaApp.jar"

spark-submit --master spark://192.168.1.7:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 Jars\Scala-IDX-Stock-Analysis-PredictData\Scala-IDX-Stock-Analysis.jar

spark-submit --master spark://192.168.1.7:7077 "D:\00 Project\00 My Project\Jars\Scala_Spark_HelloWorld_jar\Scala-Spark-HelloWorld.jar" --class TestingPrint

spark-submit --master spark://192.168.1.7:7077 "D:\00 Project\00 My Project\Jars\Scala_Spark_HelloWorld_Ultimade_jar\Scala-Spark-HelloWorld.jar" --class HelloWorld

"D:\03 Data Tools\spark-3.4.1-bin-hadoop3/bin/spark-submit.cmd" --master spark://192.168.1.7:7077 --deploy-mode client --class HelloWorld --name "spark local" "D:\00 Project\00 My Project\Jars\Scala_Spark_HelloWorld_Ultimade_jar\Scala-Spark-HelloWorld.jar"

spark-submit --master spark://192.168.1.7:7077 "D:\00 Project\00 My Project\Jars\Spark-HelloWorld\Spark_HelloWorld.jar" --class org.spark.example.HelloWorld