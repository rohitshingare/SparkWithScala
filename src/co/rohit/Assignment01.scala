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
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType

object Assignment01 extends App {
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder()
  .master("local[*]")
  .appName("App01")
  .config("spark.sql.shuffle.partitions",3) // just added this line to do not use default 200 partion as data is not that much large
  .config("spark.streaming.stopGracefullyOnShutdown","true") // gracefully stop and restart.
 // .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate()
  
    val tempSchema = StructType(List(StructField("DateTime",TimestampType),
      StructField("Temperature",FloatType),
      StructField("Humidity",FloatType),
      StructField("WindSpeed",DoubleType),
      StructField("Pressure",FloatType),StructField("Summary",StringType)))
  
   // 1 read from streaming
  val TempDf = spark
  .readStream
  .format("csv")
  .option("path","assignmentinputfolder")
  .schema(tempSchema)
  .option("maxFilesPerTrigger", 1) // it will take one file at time after 30 sec. as we have define triggering time below.
  .load
  
  TempDf.printSchema()
   
  TempDf.createOrReplaceTempView("temp")
  
  val OutputDf = spark.sql("select * from temp where WindSpeed < 11.0000 and Summary='Partly Cloud'")
  
  // 3 write to sink
  val tempQuery = OutputDf
  .writeStream
  .format("csv")
  .outputMode("append")
  .option("path", "assignmentoutputfolder")
  .option("checkpointLocation", "checkpoint-location1")
  .trigger(Trigger.ProcessingTime("30 seconds")) // it will triggered at every 30 seconds.
  .start()
  
  tempQuery.awaitTermination()
}