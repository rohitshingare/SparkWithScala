
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Practice06_Player_Ex7_MOde extends App {
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //use of mode if we found any malformed
  val orderDf = spark.read.format("json")
  .option("path", "/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/players-201019-002101.json")
  .option("mode", "FAILFAST")
  .load()
  
  orderDf.printSchema()
  orderDf.show(false)
  
  spark.stop()
  
  
}