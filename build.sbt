import Dependencies._

name := "event-sessionization"

version := "0.1"

scalaVersion := "2.12.11"
organization := "com.scribd"

libraryDependencies := Seq(
  log4j,
  log4jAPI,
  sparkCore,
  sparkSql,
  //sparkHive
  sparkSqlKafka,
 // deltaCore,
  dbutils)