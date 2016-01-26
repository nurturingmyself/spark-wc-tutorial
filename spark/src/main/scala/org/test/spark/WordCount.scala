package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * This Tutorial is done from the below URL - Marco Capuccini Tutorials
 * https://www.youtube.com/watch?v=aB4-RD_MMf0
*/

object WordCount {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")
    
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val test = sc.textFile("food.txt")
    test.flatMap { line =>
       line.split("-p")
    }
      .map {
        word => (word, 1)
      }
      .reduceByKey(_ + _)
      .saveAsTextFile("newfolder1/Food_count.txt")
  }
}