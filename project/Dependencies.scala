import sbt._

object Dependencies {
  lazy val sparkVersion = "3.0.0-preview2"

  lazy val log4j = "log4j" % "log4j" % "1.2.17"
  lazy val log4jAPI = "org.apache.logging.log4j" % "log4j-api" % "2.13.0"

  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion //% Provided
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion //% Provided
  //lazy val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion //% Provided
  lazy val sparkSqlKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion //% Provided

  // for local development - we use the open source delta API
  // this is marked `Provided` so we will use the Databricks version of delta when running in Databricks
  lazy val deltaCore = "io.delta" %% "delta-core" % "0.5.0" //% Provided

  // aws
  lazy val awsJavaSdkCore = "com.amazonaws" % "aws-java-sdk-core" % "1.11.595" % Provided
  lazy val awsJavaSdkS3 = "com.amazonaws" % "aws-java-sdk-s3" % "1.11.595" % Provided
  lazy val awsJavaSdkSts = "com.amazonaws" % "aws-java-sdk-sts" % "1.11.595" % Provided
  lazy val awsJavaSdkSecretsManager = "com.amazonaws" % "aws-java-sdk-secretsmanager" % "1.11.595"

  lazy val dbutils =  "com.databricks" % "dbutils-api_2.11" % "0.0.4"
}
