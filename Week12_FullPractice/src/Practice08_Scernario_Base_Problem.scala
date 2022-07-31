import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object Practice08_Scernario_Base_Problem extends App {
  
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "append column")
  sparkConf.set("spark.master", "local[*]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val mylist = List((1,"2023-07-25",1125,"CLOSED"),
      (2,"2025-07-25",1165,"COMPLETE"),
      (3,"2027-04-25",1335,"PENDING_PAYMEN"),
      (4,"2022-07-22",1125,"CLOSED"))
 
  
   import spark.implicits._
   
   /*val oDF = spark.sparkContext.parallelize(mylist)
   oDF.toDF()*/
   val orderDF  = spark.createDataFrame(mylist).toDF("orderid","orderdate","customerid","status")
   
   val newDf = orderDF.withColumn("orderdate", unix_timestamp(col("orderdate").cast(DateType)))
   .withColumn("newId", monotonically_increasing_id)
   .dropDuplicates("orderdate", "customerid").drop("orderid")
   .sort("orderdate")
   
   newDf.printSchema()
   newDf.show()
   spark.stop()
  
}