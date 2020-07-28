package com.scribd.models

import org.apache.spark.sql.Encoders

case class SessionInfo(userIp: String, start: java.lang.Long, end: java.lang.Long, eventsCount: Int)

object SessionInfo {
  implicit val sessionInfoSchema = Encoders.product[SessionInfo].schema
}