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

object FileSource03 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder()
  .master("local[*]")
  .appName("StreamingWordCount")
  .config("spark.sql.shuffle.partitions",3) // just added this line to do not use default 200 partion as data is not that much large
  .config("spark.streaming.stopGracefullyOnShutdown","true") // gracefully stop and restart.
  .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate()
  /* val orderSchema = StructType(List(StructField("order_id",IntegerType),
      StructField("order_date",StringType),
      StructField("order_customer_id",IntegerType),
      StructField("order_status",StringType),
      StructField("amount",IntegerType)))*/
      
  // 1 read from streaming
  val OrderDf = spark
  .readStream
  .format("json")
  .option("path","myinputfolder")
  .option("maxFilesPerTrigger", 1) // it will take one file at time after 30 sec. as we have define triggering time below.
  .load
  
  OrderDf.printSchema()
  // 2 process
  
OrderDf.createOrReplaceTempView("orders")

val completeOrders = spark.sql("select * from orders where order_status = 'COMPLETE'")
  
  // 3 write to sink
  val orderQuery = completeOrders
  .writeStream
  .format("json")
  .outputMode("append")
  .option("path", "myoutputfolder")
  .option("checkpointLocation", "checkpoint-location1")
  .trigger(Trigger.ProcessingTime("30 seconds")) // it will triggered at every 30 seconds.
  .start()
  
  orderQuery.awaitTermination()
}