import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import scala.reflect.api.materializeTypeTag
import org.apache.spark.sql.SaveMode

object Practice04_Hive_Support extends App {
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Hive Support application")
  sparkConf.set("spark.master", "local[*]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  val orderDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path","/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/orders-201025-223502.csv")
  .load()
  
 spark.sql("create database if not exists retail ")
 // we are using bucketBy to get partition pluning
 //sortBy to get data in sorted manner so, it will goint to scan olny that particular fies not all.
  orderDf
  .write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .bucketBy(4, "order_customer_id")
  .sortBy("order_customer_id")
  .saveAsTable("retail.orders")
  
  spark.catalog.listTables("retail").show()
  
  spark.stop()
  
}