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
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.TimestampType

object StaticAndStreamingJoin extends App {
  
   // setting the logging level
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //creating spark session
  val spark = SparkSession.builder()
  .master("local[*]")
  .appName("StreamingAndStaticJoin")
  .config("spark.sql.shuffle.partitions",3) // just added this line to do not use default 200 partion as data is not that much large
  .config("spark.streaming.stopGracefullyOnShutdown","true") // gracefully stop and restart.
  .getOrCreate()
  
  //schema
   val transactionSchema = StructType(List(StructField("card_id",LongType),
      StructField("amount",LongType),
      StructField("postcode",IntegerType),
      StructField("pos_id",LongType),
      StructField("transaction_dt",TimestampType)))
      
      
      //read the data from socket
      val transactionDf = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "1236")
      .load()
      
      transactionDf.printSchema()
      
      val valueDf = transactionDf.select(from_json(col("value"),transactionSchema).alias("value"))
      
      valueDf.printSchema()
      
      val refinedOrderDf = valueDf.select("value.*")
      
      refinedOrderDf.printSchema()
      
      // load static dataframe
      val memberDf = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("path", "C:/Users/DELL/Desktop/BigDataTrendyTech/Week-16/Download Practice Dataset for Week16/memberDetails.txt")
      .load()
      
      val joinExpr = refinedOrderDf.col("card_id") === memberDf.col("card_id")
      
      val jointype = "inner"
      
      val enrichedDf = refinedOrderDf.join(memberDf,joinExpr,jointype).drop(memberDf.col("card_id"))
      
      //write to sink
     
     val transactionQuery = enrichedDf.writeStream
     .format("console")
     .outputMode("update")
     .option("checkpointLocation", "checkpoint-locationSaticandstreamingJoin")
     .trigger(Trigger.ProcessingTime("15 seconds")) // it will triggered at every 30 seconds.
     .start()
  
      transactionQuery.awaitTermination()
}
