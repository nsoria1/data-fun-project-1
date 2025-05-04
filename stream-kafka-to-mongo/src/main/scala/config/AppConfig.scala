package config

import com.typesafe.config.ConfigFactory

case class KafkaConfig(bootstrapServers: String, topic: String, startingOffset: String)
case class MongoConfig(uri: String, collection: String)
case class AppConfig(
                    kafka: KafkaConfig,
                    mongo: MongoConfig,
                    checkpointLocation: String,
                    maxMessages: Option[Int]
                    )
object AppConfig {
  private val cfg = ConfigFactory.load().getConfig("app")

  def load(): AppConfig = {
    val kafkacfg = KafkaConfig(
      cfg.getString("kafka.bootstrap.servers"),
      cfg.getString("kafka.topic"),
      cfg.getString("kafka.startingOffsets")
    )

    val mongocfg = MongoConfig{
      cfg.getString("mongo.uri"),
      cfg.getString("mongo.collection")
    }

    AppConfig(
      kafka = kafkacfg,
      mongo = mongocfg,
      checkpointLocation = cfg.getString("spark.checkpointLocation"),
      maxMessages = if (cfg.hasPath("maxMessages")) Some(cfg.getInt("maxMessages")) else None
    )
  }
}