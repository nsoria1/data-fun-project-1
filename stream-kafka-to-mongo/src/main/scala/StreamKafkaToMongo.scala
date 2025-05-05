import org.apache.spark.sql.SparkSession
import config.AppConfig
import pipeline.Pipeline

object StreamKafkaToMongo {
  def main(args: Array[String]): Unit = {
    // Load configuration from file
    val config = AppConfig.load()

    // Init SparkSession
    val spark = SparkSession.builder()
      .appName("StreamKafkaToMongo")
      .master("local[*]") // Run spark in local mode
      //.config("spark.mongodb.write.connection.uri", config.mongo.uri)
      .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:src/main/resources/log4j2.properties")
      .getOrCreate()

      // Build and start pipeline
    val query = Pipeline.build(spark, config)

    query.awaitTermination()
  }
}