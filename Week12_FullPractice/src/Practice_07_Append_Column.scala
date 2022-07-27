import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.functions._

object Practice_07_Append_Column extends App {
  
  case class Person(name:String,age:Int,city:String)
  
  def ageCheck(age:Int)={
    if(age > 18 ) "Y"else "N"
  }
  
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "append column")
  sparkConf.set("spark.master", "local[*]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val df = spark.read
  .format("csv")
  .option("inferSchema", true)
  .option("path","/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/201025-223502.dataset1")
  .load()
   import spark.implicits._
 // adding header
  val df1 = df.toDF("name","age","city")
//  val ds1 = df1.as[Person]
  //ds to df conversion
 // val df2 = ds1.toDF().as[Person].toDF()
  //registering function with driver 
  //column object expression udf
 // val parseAgeFunction = udf(ageCheck(_:Int):String)
 //  df1.withColumn("adult",parseAgeFunction(col("age")))
  
 // df1.show()
 
  //SQL object expression udf
  
 // spark.udf.register("parseAgeFuction", ageCheck(_:Int):String)
  spark.udf.register("parseAgeFuction", (age:Int) => if(age > 18) "Y" else "N")
  val df2 = df1.withColumn("adult", expr("parseAgeFuction(age)"))
   df2.show()
  spark.catalog.listFunctions().filter(x => x.name == "parseAgeFuction").show()
  df1.createOrReplaceTempView("peopletable")
  spark.sql("select name,age,parseAgeFuction(age) as adult from peopletable").show
 
  spark.stop()
}