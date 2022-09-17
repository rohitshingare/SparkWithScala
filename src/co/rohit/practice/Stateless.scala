package co.rohit.practice
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
object Stateless extends App{
  
   
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkConf()
  sc.set("spark.app.name", "stateless")
  sc.set("spark.master", "local[*]")
  
  val ssc = new StreamingContext(sc,Seconds(5))
  
  val lines = ssc.socketTextStream("localhost",9998)
  
  val worlds = lines.flatMap(x => x.split(" "))
  
  val pairs = worlds.map(x => (x,1))
  
  val wordCounts = pairs.reduceByKey((x,y) => x+y)
  
  wordCounts.print()
  
  ssc.start()
  
  ssc.awaitTermination()
  
  
}