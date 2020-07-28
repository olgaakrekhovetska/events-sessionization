package com.scribd

import com.scribd.models.Event.eventJsonSchema
import com.scribd.models.{Event, SessionInfo}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.duration._

object Main {
  private val logger: Logger = LogManager.getLogger(getClass)
  private val appName = "spark-streaming-with-state"
  private val maxOffsetsPerTrigger = 100000
  private val kafkaConfig = KafkaConfig("127.0.0.1", 9092)
  private val topicName = "events"
  private val checkPointPath = "/Users/olgakrekhovetska/Documents/tmp/events"

  def main(args: Array[String]): Unit = {

    logger.info(s"Starting app, name=$appName")

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val eventsDs = readEvents
    val outputMode = OutputMode.Append()

    val eventDelay = 5.seconds
    val result = eventsDs
      .withWatermark("time", eventDelay.toString())
      .groupByKey(_.userIp)
      .flatMapGroupsWithState(outputMode, GroupStateTimeout.EventTimeTimeout())(VariableSessionWindow.sessionsF)

    val query = outputToConsole(result, outputMode).start()
    query.awaitTermination()
  }

  private def outputToConsole[T](ds: Dataset[T], outputMode: OutputMode) = {
    ds
      .writeStream
      .outputMode(outputMode)
      .format("console")
      .option("checkpointLocation", s"$checkPointPath/console")
  }

  private def readEvents(implicit spark: SparkSession): Dataset[Event] = {
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .options(kafkaConfig.properties)
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .select($"value" cast "string")
      .select(from_json($"value", eventJsonSchema).as("event"))
      .select($"event.*").as[Event]
  }
}
