import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import scala.reflect.api.materializeTypeTag

case class OrderData(order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String)

object Practice03_Order_EX4 extends App {
  
  // we get learn diff between dataframes and dataset
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val orderDf: Dataset[Row] = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/orders-201019-002101.csv")
  // this will check at runtime not in compile time - DataFrames
  orderDf.filter("order_ids < 10")
  import spark.implicits._
 
  // this will check at compile time - DataSet
  val orderDs = orderDf.as[OrderData]
 // orderDs.filter("order_ids < 10")
  orderDs.filter(x => x.order_id < 10).show();
  
  
  
  
  
  
}