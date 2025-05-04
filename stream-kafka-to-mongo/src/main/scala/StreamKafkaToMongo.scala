import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object StreamKafkaToMongo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("StreamKafkaToMongo")
      .config("spark.mongodb.write.connection.uri", sys.env("MONGO_URI"))
      .getOrCreate()

    // 1) Read data from Kafka
    val raw = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", sys.env("KAFKA_BOOTSTRAP_SERVERS"))
      .option("subscribe", sys.env("KAFKA_TOPIC"))
      .option("startingOffsets", "earliest")
      .load()

    val jsonStrings = raw.selectExpr("CAST(value AS STRING) as json")

    // 2) Define schema manually to grab from json
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("identifier", LongType),
      StructField("event", StructType(Seq(
        StructField("type", StringType)
      ))),
      StructField("date_created", StringType))
    )

    val edits = jsonStrings
      .select(from_json(col("json"), schema).as("data"))
      .select(
        col("data.name").as("name"),
        col("data.identifier").as("id"),
        col("data.event_type").as("eventType"),
        col("data.date_created").as("createdAt")
      )

    // 3) Write data into Mongo
//    val query = edits.writeStream
//      .format("com.mongo.spark.sql.DefaultSource")
//      .option("collection", sys.env("MONGO_COLLECTION"))
//      .option("checkpointLocation", "app/checkpoints")
//      .outputMode("append")
//      .start()
//
//    query.awaitTermination()
    val consoleQuery = edits.writeStream
      .format("console")
      .option("truncate", "false")
      .option("numRows", "5")
      .start()

    consoleQuery.awaitTermination()

  }
}
