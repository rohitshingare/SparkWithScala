
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Practice05_Player_Ex6 extends App {
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //Loading data from JSON 
  val orderDf = spark.read.format("json")
  .option("path", "/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/players-201019-002101.json")
  .load()
  
  orderDf.printSchema()
  orderDf.show()
  
  spark.stop()
}