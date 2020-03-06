package com.farrellw.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MinimumTemperatureByLocation {
  def main (args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the fake friends data
    val lines = sc.textFile("/Users/wgfarrell/spark/SparkScala3/1800.csv")

    val rdd = lines.map(parseLine)
    val stationTemps = rdd.filter(x => x._2 == "TMIN").map(x => (x._1, x._3))
    val minTempsByStation = stationTemps.reduceByKey((x, y) => min(x, y))
    val firstFive = minTempsByStation.collect()
    firstFive.foreach(println)
  }

  def min(fl: Float, fl1: Float): Float = {
    if(fl >= fl1){
      fl
    } else {
      fl1
    }
  }

  def parseLine(line: String): (String, String, Float) = {
    // stationId, x, entryType, temperature
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temprature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, entryType, temprature)
  }
}
