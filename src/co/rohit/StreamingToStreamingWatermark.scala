package co.rohit

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamingToStreamingWatermark extends App {
  
   // setting the logging level
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder()
  .master("local[2]").appName("join")
  .config("spark.streaming.stopGracefullyOnShutdown", "true")
  .config("spark.sql.shuffle.partitions", 3)
  .getOrCreate()
  
  //define own schema instead  of infering it
  val impressionSchema = StructType(
      List(StructField("impressionID", StringType)
      ,StructField("ImpressionTime", TimestampType)
      ,StructField("CampaignName", StringType)))
      
      val clickSchema = StructType(
          List(StructField("clickID", StringType)
              ,StructField("ClickTime", TimestampType)))
              
              //read the stream
              val   impressionsDf =spark.readStream.format("socket")
              .option("host", "localhost").option("port", "12342").load()
              
              val clicksDf = spark.readStream .format("socket") 
              .option("host", "localhost").option("port", "12343").load()
              
              //structure the data based on the schema defined - impressionDf
              val valueDF1 = impressionsDf.select(from_json(col("value"), 
                  impressionSchema).alias("value"))
                  
                  val impressionDfNew = valueDF1.select("value.*").withWatermark("impressionTime","30 minute")
                  
                  //structure the data based on the schema defined -clickDf
                  val valueDF2 = clicksDf.select(from_json(col("value"), clickSchema)
                      .alias("value"))
                      val clickDfNew = valueDF2.select("value.*").withWatermark("clickTime","30 minute")
                      
                      //join condition
                      val joinExpr = impressionDfNew.col("ImpressionID")=== clickDfNew.col("clickID")
                      
                      //join type
                      val joinType="inner"
                      
                      //joining both the streaming data frames
                      val joinedDf = impressionDfNew.join(clickDfNew,joinExpr,joinType)
                      .drop(clickDfNew.col("clickID"))
                      
                      //output to the sink
                      val campaignQuery =joinedDf
                      .writeStream.format("console")
                      .outputMode("append").option("checkpointLocation", "chk-Loc2")
                      .trigger(Trigger.ProcessingTime("15 second"))
                      .start()
                      
                      campaignQuery.awaitTermination()
}
