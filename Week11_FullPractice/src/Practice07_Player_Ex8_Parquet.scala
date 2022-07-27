import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Practice07_Player_Ex8_Parquet extends App{
 
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //use of mode if we found any malformed
  val orderDf = spark.read
  .option("path", "/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/users-201019-002101.parquet")
  .load()
  
  orderDf.printSchema()
  orderDf.show(false)
  
  spark.stop()
  
}