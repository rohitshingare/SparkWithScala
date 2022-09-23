package co.rohit
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
//import org.apache.spark.sql.functions._
object Assignment02 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
/**CREATING A SPARK~SESSION*/
  val spark = SparkSession.builder().master("local[2]").appName("App2").config("spark.streaming.stopGracefullyOnShutdown", "true").config("spark.sql.shuffle.partitions", 3).getOrCreate()
  
  val weatherSchema = StructType(List(StructField("DateTime", TimestampType),StructField("Temperature", DoubleType),StructField("Humidity", FloatType),StructField("WindSpeed", DoubleType),StructField("Pressure", DoubleType),StructField("Summary", StringType)))
  
  //1. READ FROM THE STREAM
  val weatherSocketDF = spark.readStream.format("socket").option("host", "localhost")
  .option("port", "7817").load()
  
  /**processing*/
  val valueDF = weatherSocketDF.select(from_json(col("value"), weatherSchema).alias("Value"))
  
  val refinedweatherSocketDF = valueDF.select("Value.*")
  
  refinedweatherSocketDF.printSchema()
  
  val windowAggWeatherSocketDF = refinedweatherSocketDF.withWatermark("DateTime", "30 minute")  //<------"WATERMARK" MENTION HERE
  .groupBy(window(col("DateTime"), "15 minute", "5 minute")).agg(avg("Temperature").alias("totalTemperature"))
  
  val outputWeatherSocketDF = windowAggWeatherSocketDF.select("window.start", "window.end", "totalTemperature")
  
  outputWeatherSocketDF.printSchema()
  
  
  /**write to the sink console*/
  
  val WeatherSocketQuery = outputWeatherSocketDF.writeStream.format("console")//    .format("csv")
  .outputMode("update")//.outputMode("append")
  .option("checkpointLocation", "checkpoint-Location3")
  .trigger(Trigger.ProcessingTime("15 seconds"))
  .start()
  WeatherSocketQuery.awaitTermination()

}