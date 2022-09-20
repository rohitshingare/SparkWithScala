package co.rohit

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

object FileSourceCount04 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder()
  .master("local[*]")
  .appName("StreamingWordCount")
  .config("spark.sql.shuffle.partitions",3) // just added this line to do not use default 200 partion as data is not that much large
  .config("spark.streaming.stopGracefullyOnShutdown","true") // gracefully stop and restart.
  .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate()
 
  // 1 read from streaming
  val OrderDf = spark
  .readStream
  .format("json")
  .option("path","myinputfolder")
  .load
  
  OrderDf.printSchema()
  // 2 process
  
OrderDf.createOrReplaceTempView("orders")

val completeOrders = spark.sql("select count(*) from orders where order_status = 'COMPLETE'")
  
  // 3 write to sink
  val orderQuery = completeOrders
  .writeStream
  .format("console")
  .outputMode("complete")
  .option("checkpointLocation", "checkpoint-location2")
  .trigger(Trigger.ProcessingTime("30 seconds")) // it will triggered at every 30 seconds.
  .start()
  
  orderQuery.awaitTermination()
}