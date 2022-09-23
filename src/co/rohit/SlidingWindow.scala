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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object SlidingWindow extends App {
   // setting the logging level
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //creating spark session
  val spark = SparkSession.builder()
  .master("local[*]")
  .appName("StreamingWordCount")
  .config("spark.sql.shuffle.partitions",3) // just added this line to do not use default 200 partion as data is not that much large
  .config("spark.streaming.stopGracefullyOnShutdown","true") // gracefully stop and restart.
  .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate()
  
  val orderSchema = StructType(List(StructField("order_id",IntegerType),
      StructField("order_date",TimestampType),
      StructField("order_customer_id",IntegerType),
      StructField("order_status",StringType),
      StructField("amount",IntegerType)))
      
      //read the data from socket
      
      
      val orderDf = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "1236")
      .load()
      
      orderDf.printSchema()
      
      val valueDf = orderDf.select(from_json(col("value"),orderSchema).alias("value"))
      
      valueDf.printSchema()
      
      val refinedOrderDf = valueDf.select("value.*")
      
      refinedOrderDf.printSchema()
      
      val windowAggDf = refinedOrderDf
      .withWatermark("order_date", "30 minute")
      .groupBy(window(col("order_date"), "15 minute","5 minute")) // sliding of 5 minute 
      .agg(sum("amount").alias("InvoiceTotal"))
      
     val outputDf = windowAggDf.select("window.start","window.end", "InvoiceTotal")
     
     //write to sink
     
     val orderQuery = outputDf.writeStream
     .format("console")
     .outputMode("update")
     .option("checkpointLocation", "checkpoint-locationWaterMarks")
  .trigger(Trigger.ProcessingTime("30 seconds")) // it will triggered at every 30 seconds.
  .start()
  
  orderQuery.awaitTermination()
}