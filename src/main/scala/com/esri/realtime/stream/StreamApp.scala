package com.esri.realtime.stream

import org.apache.spark.sql.SparkSession

object StreamApp {

  def main(args: Array[String]) {
    val logFile = "/Library/Apache/spark-2.4.0-bin-hadoop2.7/README.md"
    val spark = SparkSession.builder.appName("Simple Apache Spark Streaming Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
