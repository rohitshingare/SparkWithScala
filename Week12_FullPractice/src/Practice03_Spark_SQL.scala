import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import scala.reflect.api.materializeTypeTag
import org.apache.spark.sql.SaveMode

object Practice03_Spark_SQL extends App {
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val orderDf: Dataset[Row] = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path","/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/orders-201025-223502.csv")
  .load()
  
  orderDf.createOrReplaceTempView("orders")
  //Example 1
//  val resultDf = spark.sql("select order_status,count(*) as count from orders group by order_status order by count desc ")
  
//  resultDf.show()
  
    //Example 2
  val result_ex2 = spark.sql("select order_customer_id,count(*) as count from orders group by order_customer_id "+
      "having count > 1000 where order_status='CLOSED'  order by count"  )
  result_ex2.show
}