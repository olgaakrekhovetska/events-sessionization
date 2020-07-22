package com.scribd

import com.scribd.models.Event.eventJsonSchema
import com.scribd.models.{Event, SessionInfo}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.duration._

object Main {
  private val logger: Logger = LogManager.getLogger(getClass)
  private val appName = "spark-streaming-with-state"
  private val maxOffsetsPerTrigger = 100000
  private val kafkaConfig = KafkaConfig("127.0.0.1", 9092)
  private val topicName = "events"
  private val checkPointPath = "/Users/olgakrekhovetska/Documents/tmp/1"

  def main(args: Array[String]): Unit = {

    logger.info(s"Starting app, name=$appName")

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val eventsDs = readEvents

    val delayThreshold = 10.seconds
    //val eventsWatermarked = eventsDs.withWatermark("time", delayThreshold.toString)

    val outputMode = OutputMode.Append
    val result = eventsDs.groupByKey(_.userIp)
      .flatMapGroupsWithState(
        outputMode,
        GroupStateTimeout.NoTimeout)(userIpSessions)

    val query = outputToConsole(result, outputMode)
      .start()

    query.awaitTermination()
  }

  def userIpSessions(userIp: String, events: Iterator[Event], state: GroupState[SessionInfo]): Iterator[SessionInfo] = {
    val values = events.toSeq
    logger.info(s"UserIp: $userIp")
    logger.info(s"Events (${values.size}):")
    values.zipWithIndex.foreach { case (v, idx) => logger.info(s"$idx. $v") }
    logger.info(s"State: $state")

    val maxEventTime = values.map(_.time).max
    val minEventTime = values.map(_.time).min

    val currentState = state.getOption match {
      case Some(previousState) =>
        val newSessionStart = if (previousState.start < minEventTime) previousState.start else minEventTime
        val newSessionEnd = if (previousState.end < maxEventTime) maxEventTime else previousState.end
        SessionInfo(previousState.userIp, newSessionStart, newSessionEnd)
      case None => SessionInfo(userIp, minEventTime, maxEventTime)
    }

    state.update(currentState)
    Iterator(currentState)
  }

  private def outputToConsole[T](ds: Dataset[T], outputMode: OutputMode) = {
    ds
      .writeStream
      .outputMode(outputMode)
      .format("console")
      .option("checkpointLocation", s"$checkPointPath/console")
      .trigger(Trigger.ProcessingTime(1.seconds))
  }

  private def readEvents(implicit spark: SparkSession): Dataset[Event] = {
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .options(kafkaConfig.properties)
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .select($"value" cast "string")
      .select(from_json($"value", eventJsonSchema).as("event"))
      .select($"event.*").as[Event]
  }
}
