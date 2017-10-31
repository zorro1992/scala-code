package com.unwindinghadoop.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.util.matching
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import scala.collection.JavaConverters._

object friends {
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "friends")
    
    //Loadin the data 
    val lines = sc.textFile("../friends.csv")
    
    // Mapping each input and also tell the data is seperated by , and take only 3rd filed 
    val lines2 = lines.map(x => x.toString().split(",")(3))

    
    //Sorting the data in asc order 
    val lines3 = lines2.map(_.sortWith(_<_))
    
    //Saving the result in the local machine
    lines3.saveAsTextFile("../results")
    println(lines3.collect().mkString(" "))
    
  }
}
