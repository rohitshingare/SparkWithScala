import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object Practice09_Aggrgate_Transformation_Simple extends App {
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "append column")
  sparkConf.set("spark.master", "local[*]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val orderDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path","/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/order_data-201025-223502.csv")
  .load()
  
  orderDf.select(count("*").as("RowCount"),sum("Quantity").as("TotalQuantity"),avg("UnitPrice").as("AvgPrice"),
      countDistinct("InvoiceNo").as("CountDistinct")).show()
  spark.stop()
  
  orderDf.selectExpr(
      "count(*) as rowcount",
      "sum(Quantity) as totalQuantity",
      "avg(UnitPrice) as AvgPrice",
      "count(Distinct(InvoiceNo))").show
  
orderDf.createOrReplaceTempView("invoice")
//spark.sql("select count(*), sum(Quantity),avg(UnitPrice),count(Distinct(InvoiceNo)) from invoice").show
}