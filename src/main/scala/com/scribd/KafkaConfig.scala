package com.scribd

import java.util.Properties
import KafkaConfig._
import org.apache.kafka.clients.consumer.ConsumerConfig
import scala.collection.JavaConverters._

case class KafkaConfig(host: String, port: Int) {
  val properties = {
    val props = new Properties()
    props.put(s"${KafkaPropertyPrefix}${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}", s"$host:$port")
    props.asScala.toMap
  }
}

object KafkaConfig {
  val KafkaPropertyPrefix = "kafka."
}
