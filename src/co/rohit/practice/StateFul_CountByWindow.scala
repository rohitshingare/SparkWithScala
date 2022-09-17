package co.rohit.practice

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object StateFul_CountByWindow extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkConf()
  sc.set("spark.app.name", "stateful word count window with countbywindow")
  sc.set("spark.master", "local[*]")
  
  val ssc = new StreamingContext(sc,Seconds(5))
  
  val lines = ssc.socketTextStream("localhost",9998)
  
 
  ssc.checkpoint(".")
  
  val wordCounts = lines.countByWindow(Seconds(10) , Seconds(5))
  
  wordCounts.print()
  
  ssc.start()
  
  ssc.awaitTermination()
}