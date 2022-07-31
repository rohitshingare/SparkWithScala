import org.apache.spark.SparkConf
//import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.log4j.Level 
import org.apache.log4j.Logger

object Practice16_Pivot_table extends App {
  case class Logging(level:String,datetime:String)
   Logger.getLogger("org").setLevel(Level.ERROR)
  def mapper(line:String):Logging={
   val fields = line.split(",")
    val logging : Logging = Logging(fields(0),fields(1))
    return logging
  }
  
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "append column")
  sparkConf.set("spark.master", "local[*]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val df3 = spark.read.option("header", true).csv("/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/biglog-201105-152517.txt")

 df3.createOrReplaceTempView("my_new_logging_table")
 
 val results = spark.sql("""select level,date_format(datetime,'MMMM') as month,count(1) as total from my_new_logging_table
   group by level,month""")
   
   /*val result1 = spark.sql("""select level,
     date_format(datetime,'MMMM') as month,
     cast(date_format(datetime,'M') as int) as monthnum
      from my_new_logging_table
  """).groupBy("level").pivot("monthnum").count().show()*/
   
   val columns = List("January","February","March","April","May","June","July","August","September","October","November","December")
   
    val result1 = spark.sql("""select level,
     date_format(datetime,'MMMM') as month
      from my_new_logging_table
  """).groupBy("level").pivot("month",columns).count().show()
  

spark.stop()
  
}