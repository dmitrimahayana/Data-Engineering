ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "Scala_Amazon_DataMining",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.0",
      "org.apache.spark" %% "spark-sql" % "3.3.0",
      "org.apache.spark" %% "spark-mllib" % "3.3.0",
      "org.postgresql" % "postgresql" % "9.4-1206-jdbc42"
    )

  )
