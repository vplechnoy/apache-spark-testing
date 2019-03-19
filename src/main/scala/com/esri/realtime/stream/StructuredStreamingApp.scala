package com.esri.realtime.stream

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object StructuredStreamingApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredStreamingApp")
      .getOrCreate()

    import spark.implicits._

    val df = spark.readStream
      .format("socket")
      .option("host", "appropriate-nc")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val ds = df.as[String]
    val words = ds.flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "/data")
      .format("memory")
      .queryName("memCount")
      .start()

    query.awaitTermination()
  }
}
