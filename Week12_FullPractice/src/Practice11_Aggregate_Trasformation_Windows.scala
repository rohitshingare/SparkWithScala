import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.log4j.Level 
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window

object Practice11_Aggregate_Trasformation_Windows extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "append column")
  sparkConf.set("spark.master", "local[*]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val invoiceDf = spark.read
  .format("csv")
  .option("inferSchema", true)
  .option("path","/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/windowdata-201025-223502.csv")
  .load()
  
  val myWindow = Window.partitionBy("country").orderBy("weeknum")
  .rowsBetween(Window.unboundedPreceding,Window.currentRow)
  
  invoiceDf.toDF("country","weeknum","numinvoices","totalquantity","invoicevalue")
  .withColumn("RunningTotal",sum("invoicevalue").over(myWindow)).show()
  spark.stop()
}