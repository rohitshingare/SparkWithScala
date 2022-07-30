
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
object Practice10_Aggregate_Tranformation_Group extends App  {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "append column")
  sparkConf.set("spark.master", "local[*]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val invoiceDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path","/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/order_data-201025-223502.csv")
  .load()
  
  val summaryDf = invoiceDf.groupBy("Country", "InvoiceNo")
  .agg(sum("Quantity").as("TotalQuantuty"),sum(expr("Quantity * UnitPrice")).as("InvoiceValue")
      )
      summaryDf.show()
  val summaryDf1 = invoiceDf.groupBy("Country", "InvoiceNo")
  .agg(expr("sum(Quantity) as TotalQuality"),
      expr("sum(Quantity * UnitPrice) as InvoiceValue"))
      
    summaryDf1.show()
    invoiceDf.createOrReplaceTempView("invoice")
    val summaryDf2 = spark.sql("""select Country,InvoiceNo,sum(Quantity) as TatalQuantity, sum(Quantity * UnitPrice) as 
      InvoiceValue from invoice group by Country, InvoiceNo""").show()
  spark.stop()
  
}