ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

lazy val root = (project in file("."))
  .settings(
    name := "StreamKafkaToMongo",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.3.2",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.1",
      "io.github.cdimascio" % "java-dotenv" % "3.2.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.17.2"
    ),
    Compile / mainClass := Some("StreamKafkaToMongo")
  )