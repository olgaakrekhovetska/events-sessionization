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

  private val periodOfInactivity: java.lang.Long = 10000L //10 secs

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
      .flatMapGroupsWithState(outputMode, GroupStateTimeout.EventTimeTimeout())(sessionsF)

    val query = outputToConsole(result, outputMode).start()
    query.awaitTermination()
  }

  def sessionsF(key: String, values: Iterator[Event], state: GroupState[SessionInfo]): Iterator[SessionInfo] = {
    //when no events session should expire on new batch for any other key
    if (values.isEmpty && state.hasTimedOut) {
      logger.info(s"State for key=$key expired, state=${state.get}, currentWatermark: ${state.getCurrentWatermarkMs()}.")
      state.remove()
      Iterator.empty
    } else {
      logger.info(s"Got values for key=$key, state=${state.getOption}, currentWatermark: ${state.getCurrentWatermarkMs()}.")
      // sort events by time in ascending order to update session iteratively with bigger event time
      val events = values.toSeq.sortBy(_.time.getTime).toList
      events.zipWithIndex.foreach { case (v, idx) => logger.info(s"$idx. $v") }

      val updatedSession = state.getOption match {
        case None =>
          events match {
            case head :: tail =>
              val newSession = SessionInfo(key, head.time.getTime, head.time.getTime, 1)
              logger.info(s"Created new state for key=${key}, state=${newSession}, currentWatermark=${state.getCurrentWatermarkMs()}.")
              Some(updateSessionWithEvents(newSession, tail))
            case _ => //if there are no events then we do not need to create session
              None
          }
        case Some(existingSession) => Some(updateSessionWithEvents(existingSession, events))
      }

      updatedSession match {
        case Some(updatedSession) =>
          state.update(updatedSession)
          val expirationTime = updatedSession.end + periodOfInactivity
          state.setTimeoutTimestamp(expirationTime)
          logger.info(s"Updated state for key=${key}, state=${updatedSession}, expirationTime=$expirationTime, currentWatermark=${state.getCurrentWatermarkMs()}.")
          Iterator(updatedSession)
        case None => Iterator.empty
      }
    }
  }

  /** Update initial session state with events time, event should be sorted ih ASC order. */
  private def updateSessionWithEvents(initialSessionState: SessionInfo, events: List[Event]): SessionInfo = {
    events.foldLeft(initialSessionState) { case (session, event) =>
      val eventTimeMs: java.lang.Long = event.time.getTime
      eventTimeMs match {
        case eventTime if (session.start <= eventTime && eventTime <= (session.end + periodOfInactivity)) =>
          logger.info(s"Event=$event is in scope of current session=$session.")
          val newSessionStart = if (session.start < eventTime) session.start else eventTime
          val newSessionEnd = if (session.end < eventTime) eventTime else session.end
          val updatedSession = session.copy(start = newSessionStart, end = newSessionEnd, eventsCount = session.eventsCount + 1)
          updatedSession
        case _ =>
          logger.info(s"Event=$event is out of scope for current session=$session.")
          //TODO this should be handeled in a way of creating new session and return list of sessions -- old session and new
          session
      }
    }
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
