package co.rohit.practice

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object StateFul_Sliding_Window_SmallChange extends App {
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkConf()
  sc.set("spark.app.name", "stateful word count window")
  sc.set("spark.master", "local[*]")
  
  val ssc = new StreamingContext(sc,Seconds(5))
  
  val lines = ssc.socketTextStream("localhost",9998)
  
 
  ssc.checkpoint(".")
  
  
  val worlds = lines.flatMap(x => x.split(" "))
  
  val pairs = worlds.map(x => (x,1))
  
  def summary(x :Int, y: Int) = x + y;
   
  def inverse(x :Int, y: Int) = x - y;
  
  val wordCounts = pairs.reduceByKeyAndWindow(summary(_,_),inverse(_,_) ,Seconds(10) , Seconds(5)).filter(x => x._2 > 0)
  
  wordCounts.print()
  
  ssc.start()
  
  ssc.awaitTermination()
  
}