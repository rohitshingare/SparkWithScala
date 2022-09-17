package co.rohit.practice

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object StateFul_Window_ReduceByMethod extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkConf()
  sc.set("spark.app.name", "stateful word count window with reducebywindow")
  sc.set("spark.master", "local[*]")
  
  val ssc = new StreamingContext(sc,Seconds(5))
  
  val lines = ssc.socketTextStream("localhost",9998)
  
 
  ssc.checkpoint(".")
  
  
 
  
  def summary(x :String, y: String) ={ (x.toInt + y.toInt).toString()}
   
  def inverse(x :String, y: String) = {(x.toInt - y.toInt).toString()}
  
  val wordCounts = lines.reduceByWindow(summary(_,_),inverse(_,_) ,Seconds(10) , Seconds(5))
  
  wordCounts.print()
  
  ssc.start()
  
  ssc.awaitTermination()
  
}