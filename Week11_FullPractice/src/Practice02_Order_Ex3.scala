
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object Practice02_Order_Ex3 extends App {
   
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val orderDf = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/orders-201019-002101.csv")
  
  val groupByOrder = orderDf
  .repartition(4)
  .where("order_customer_id > 1000")
  .select("order_id","order_customer_id")
  .groupBy("order_customer_id")
  .count()
  
  groupByOrder.show(50)
  
  Logger.getLogger(getClass.getName).info("my application is successful")
  
  scala.io.StdIn.readLine()
  spark.stop()
}