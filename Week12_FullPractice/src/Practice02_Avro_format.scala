import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import scala.reflect.api.materializeTypeTag
import org.apache.spark.sql.SaveMode

object Practice02_Avro_format extends App{
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
  
  print("order has"+orderDf.rdd.getNumPartitions);
  val orderRes = orderDf.repartition(2)
   print("order has"+orderRes.rdd.getNumPartitions);
  
  orderDf.write
  .format("avro")
  .partitionBy("order_status")
  .mode(SaveMode.Overwrite)
  .option("maxRecordPerFile", 2000)
  .option("path", "/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/Output")
  .save()
  
  spark.close()
  
}