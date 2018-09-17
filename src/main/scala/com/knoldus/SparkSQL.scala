
package com.knoldus

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQL extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("Power-Plant")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val customSchema = StructType(Array(StructField("timeStamp", StringType, true), StructField("temperature", FloatType, true)))

  val df = spark.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .schema(customSchema)
    .load("data.csv")
  val function = udf(getTimeFromTimestamp _, StringType)

  private def getTimeFromTimestamp(timeStamp: String) = {
    val time = timeStamp.slice(11, 16)
    time
  }

  df.withColumn("date", to_date(df.col("timeStamp")))
    .withColumn("time", function(df.col("timeStamp")))
    .write
    .partitionBy("date", "time")
    .mode(SaveMode.Ignore)
    .parquet("power_plant_analysis_result.parquet")


}
