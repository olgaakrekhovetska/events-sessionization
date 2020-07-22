package com.scribd.models

import org.apache.spark.sql.Encoders

case class Event(userIp: String, name: String, time: java.lang.Long)

object Event {
  /*
    val schema = new StructType()
      .add("userIp", StringType)
      .add("name", StringType)
      .add("time", LongType)*/
  val eventJsonSchema = Encoders.product[Event].schema
}