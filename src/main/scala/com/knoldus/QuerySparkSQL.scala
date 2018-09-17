package com.knoldus

import org.apache.spark.sql.SparkSession

object QuerySparkSQL extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("Power-Plant-thermostat")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  spark.read
    .parquet("power_plant_analysis_result.parquet")
    .createOrReplaceTempView("power_plant")

  val sqlDF = spark.sql("SELECT temperature FROM power_plant WHERE date BETWEEN '2010-02-26' and '2010-02-28'")
  sqlDF.show()

  val sqlDF1 = spark.sql("SELECT temperature FROM power_plant WHERE time BETWEEN '03:33' and '08:45'")
  sqlDF1.show()
}
