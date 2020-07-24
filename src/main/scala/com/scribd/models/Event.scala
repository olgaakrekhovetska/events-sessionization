package com.scribd.models

import java.sql.Timestamp

import org.apache.spark.sql.Encoders

case class Event(userIp: String, name: String, time: Timestamp)

object Event {
  /*
    val schema = new StructType()
      .add("userIp", StringType)
      .add("name", StringType)
      .add("time", LongType)*/
  val eventJsonSchema = Encoders.product[Event].schema
}