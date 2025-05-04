ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "StreamKafkaToMongo",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.3.2",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.1",
      "com.typesafe" % "config" % "1.4.3"),
    mainClass := Some("app.StreamKafkaToMongo")
  )
