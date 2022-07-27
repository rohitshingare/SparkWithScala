import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Practice04_Order_Ex5 extends App  {
  
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //standarized format
  val orderDf = spark.read.format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", "/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/orders-201019-002101.csv")
  .load()
  
  orderDf.printSchema()
  orderDf.show()
  
  spark.stop()
  
}