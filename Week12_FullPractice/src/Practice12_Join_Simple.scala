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

object Practice12_Join_Simple extends App {
  
   Logger.getLogger("org").setLevel(Level.ERROR)
   
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
  .option("path","/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/orders-201025-223502.csv")
  .load()
  val customerDf = spark.read
  .format("csv")
    .option("header", true)
    .option("inferSchema", true)
  .option("path","/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/customers-201025-223502.csv")
  .load()
  
  val joinCondition = orderDf.col("order_customer_id") === customerDf.col("customer_id")
   
  val joinType = "outer" // it can be outer,right,full
  
  val JoinedDf = orderDf.join(customerDf,joinCondition,joinType).sort("order_customer_id")
JoinedDf.show
 spark.stop()
  
  
  
  
}