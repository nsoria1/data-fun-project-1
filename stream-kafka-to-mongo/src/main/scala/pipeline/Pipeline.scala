package pipeline

import config.AppConfig
import schema.Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pipeline {
  private val logger = org.apache.log4j.Logger.getLogger(getClass)
  /**
   * Builds and start and streaming query:
   * 1) Read json from Kafka topic
   * 2) Parses using predefined schema
   * 3) Select and rename fields
   * 4) Write to Mongo or Console
   */
  def build(spark: SparkSession, config: AppConfig) = {
    import spark.implicits._

    logger.info(s"Starting pipeline reading from topic ${config.kafka.topic} with startingOffsets=${config.kafka.startingOffset}")

    // Read from Kafka
    val raw: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", config.kafka.topic)
      .option("startingOffsets", config.kafka.startingOffset)
      .load()
    logger.info("Kafka stream loaded, casting value to string")

    // Extract JSON string
    val jsonString = raw.selectExpr("CAST(value AS STRING) AS json")
    logger.info("Parsing JSON with predefined schema")

    // Parse JSON with schema pre defined
    val parsed = jsonString
      .select(from_json(col("json"), Schema.editSchema).as("data"))

    // Extract fields
    val edits = parsed.select(
      col("data.name").as("name"),
      col("data.identifier").as("id"),
      col("data.event.type").as("eventType"),
      col("data.date_created").as("createdAt")
    )

    // Sink: Write to Mongo
    // val query = edits.writeStream
    //   .format("com.mongodb.spark.sql.DefaultSource")
    //   .option("collection", config.mongo.collection)
    //   .option("checkpointLocation", config.checkpointLocation)
    //   .outputMode("append")
    //   .start()

    // Testing output
    logger.info("Starting console sink to display 2 rows per micro-batch")
    val consoleQuery = edits.writeStream
      .format("console")
      .option("truncate", "false")
      .option("numRows", "2")
      .option("checkpointLocation", config.checkpointLocation)
      .start()

    consoleQuery
  }
}
