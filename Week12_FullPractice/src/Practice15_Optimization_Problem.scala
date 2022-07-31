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

object Practice15_Optimization_Problem extends App{
 
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
  

  
  /*val mylist = List("DEBUG,2015-2-6 16:24:07",
"WARN,2016-7-26 18:54:43",
"INFO,2012-10-18 14:35:19",
"DEBUG,2012-4-26 14:26:50",
"ERROR,2015-6-28 19:25:05")
 import spark.implicits._
 
val rdd1 = spark.sparkContext.parallelize(mylist)

val rdd2 = rdd1.map(mapper)

val df1 = rdd2.toDF()

df1.createOrReplaceTempView("logging_table")

//spark.sql("select * from logging_table")
//spark.sql("select level,collect_list(datetime) from logging_table group by level order by level").show(false)

val df2 = spark.sql("select level,date_format(datetime,'MMMM') as month from logging_table")

df2.createOrReplaceTempView("new_logging_table")

spark.sql("select * from new_logging_table ").show*/
 
 val df3 = spark.read.option("header", true).csv("/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/biglog-201105-152517.txt")

 df3.createOrReplaceTempView("my_new_logging_table")
 
 val results = spark.sql("""select level,date_format(datetime,'MMMM') as month,count(1) as total from my_new_logging_table
   group by level,month""")
   
   val result1 = spark.sql("""select level,
     date_format(datetime,'MMMM') as month,
     cast(first(date_format(datetime,'M')) as int) as monthnum,
     count(1) as total 
     from my_new_logging_table
   group by level,month
   order by monthnum""")
   
   val result2 = result1.drop("monthnum").show(60)

spark.stop()
  
}