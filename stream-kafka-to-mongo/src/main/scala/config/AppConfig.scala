package config

import io.github.cdimascio.dotenv.Dotenv

case class KafkaConfig(bootstrapServers: String, topic: String, startingOffset: String)
case class MongoConfig(uri: String, collection: String)
case class AppConfig(
                      kafka: KafkaConfig,
                      mongo: MongoConfig,
                      checkpointLocation: String,
                      maxMessages: Option[Int]
                    )

object AppConfig {

  // Initialize dotenv
  private val dotenv: Dotenv = Dotenv.configure()
    .directory(".")
    //.ignoreIfMissing()
    //.ignoreIfMalformed()
    .load()

  def load(): AppConfig = {

    // Helper function to get env variable with validation
    def getEnvOrThrow(key: String): String = {
      val value = Option(dotenv.get(key)).orElse(sys.env.get(key))
        .getOrElse(throw new IllegalArgumentException(s"$key is not defined"))
      if (value.isEmpty) throw new IllegalArgumentException(s"$key is empty")
      value
    }

    // Load required variables
    val bootstrapServers = getEnvOrThrow("KAFKA_BOOTSTRAP_SERVERS")
    val topic = getEnvOrThrow("KAFKA_TOPIC")
    val mongoUri = getEnvOrThrow("MONGO_URI")
    val mongoCollection = getEnvOrThrow("MONGO_COLLECTION")

    // Load optional/default variables
    val startingOffset = Option(dotenv.get("KAFKA_STARTING_OFFSET"))
      .orElse(sys.env.get("KAFKA_STARTING_OFFSET"))
      .getOrElse("earliest")
    val checkpointLocation = Option(dotenv.get("SPARK_CHECKPOINT_LOCATION"))
      .orElse(sys.env.get("SPARK_CHECKPOINT_LOCATION"))
      .getOrElse("/app/checkpoints")
    val maxMessages = Option(dotenv.get("MAX_MESSAGES"))
      .orElse(sys.env.get("MAX_MESSAGES"))
      .map(_.toInt)

    val kafkaConfig = KafkaConfig(
      bootstrapServers,
      topic,
      startingOffset
    )

    val mongoConfig = MongoConfig(
      mongoUri,
      mongoCollection
    )

    AppConfig(
      kafka = kafkaConfig,
      mongo = mongoConfig,
      checkpointLocation = checkpointLocation,
      maxMessages = maxMessages
    )
  }
}