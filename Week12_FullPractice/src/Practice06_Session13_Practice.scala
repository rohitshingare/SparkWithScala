import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.functions._


object Practice06_Session13_Practice extends App {
  //how to refer columns in dataset/dataframes
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "refering of columns")
  sparkConf.set("spark.master", "local[*]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val orderDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path","/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/orders-201019-002101.csv")
  .load()
  //column string
  orderDf.select("order_id", "order_status").show
  //column object
  import spark.implicits._
orderDf.select(column("order_id"),col("order_status"),$"order_customer_id",'order_status).show
//below is not possible
  //orderDf.select("order_id", col("order_status"))
//not possible
//orderDf.select("order_id", "concat(order_status,'_STATUS')").show()
  orderDf.select(col("order_id"), expr("concat(order_status,'_STATUS')")).show(false)
  orderDf.selectExpr("order_id", "concat(order_status,'_STATUS')").show()
  spark.stop()
}