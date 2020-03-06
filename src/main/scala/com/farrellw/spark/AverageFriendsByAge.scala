package com.farrellw.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AverageFriendsByAge {
  def main (args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the fake friends data
    val lines = sc.textFile("/Users/wgfarrell/spark/SparkScala3/fakeFriends.csv")
    val rdd = lines.map(parseLine)
    val friendByAge = rdd.mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val averageFriendsByAge = friendByAge.mapValues(x => x._1 / x._2).sortBy(_._2)
    val firstFive = averageFriendsByAge.take(5)
    firstFive.foreach(println)
  }

  def parseLine(line: String): (String, Int) = {
    // 0, Will, 33, 385
    val fields = line.split(",")
    val age = fields(1)
    val numFriends = fields(3).toInt
    (age, numFriends)
  }
}
