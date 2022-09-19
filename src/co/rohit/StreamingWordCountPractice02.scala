package co.rohit

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming.Seconds

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StreamingWordCountPractice02 extends App{
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder()
  .master("local[*]")
  .appName("StreamingWordCount")
  .config("spark.sql.shuffle.partitions",3) // just added this line to do not use default 200 partion as data is not that much large
  .config("spark.streaming.stopGraceFullyOnShutdown","true") // gracefully stop and restart.
  .getOrCreate()
  
  // 1 read from streaming
  val linesDf = spark
  .readStream
  .format("socket")
  .option("host","0.0.0.0")
  .option("port", "9999")
  .load
  
  linesDf.printSchema()
  // 2 process
  
  val wordDf = linesDf.selectExpr("explode(split(value,' ')) as word")
  val CountDf = wordDf.groupBy("word").count()
  
  // 3 write to sink
  val wordCountQuery = CountDf
  .writeStream
  .format("console")
  .outputMode("complete")
  .option("checkpointLocation", "checkpoint-location1")
  .trigger(Trigger.ProcessingTime("30 seconds")) // it will triggered at every 30 seconds.
  .start()
  
  wordCountQuery.awaitTermination()
}