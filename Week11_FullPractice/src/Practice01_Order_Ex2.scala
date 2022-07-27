
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Practice01_Order_Ex extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
  //https://www.onlinegdb.com/fork/OS7MGBQ4V
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val orderDf = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/orders-201019-002101.csv")
  
  orderDf.show()
  
  orderDf.printSchema()
  
  scala.io.StdIn.readLine()
  spark.stop()
  
  
}