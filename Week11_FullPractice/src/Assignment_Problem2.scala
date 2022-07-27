import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object Assignment_Problem2 extends App {
  
  // Setting the Logging~Level To ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)
  // define a schema for employee record data using a case class
  case class windowData(Country: String, Weeknum: Int, NumInvoices: Int, TotalQuantity: Int, InvoiceValue: String)
  //Create Spark Config Object
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "WEEK11_SOLUTION_2_WINDOWDATA")
  sparkConf.set("spark.master", "local[2]")
  //Create Spark Session
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._
  val windowDataDF = spark.sparkContext.textFile("/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/windowdata-201021-002706.csv")
  .map(_.split(","))
  .map(e => windowData(e(0), e(1).trim.toInt, e(2).trim.toInt,e(3).trim.toInt, e(4)))
  .toDF()
  .repartition(8)
  
  windowDataDF.write.format("json").mode(SaveMode.Overwrite).option("path", "/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/windowData_jsonoutput").save()
  windowDataDF.show()
  spark.stop()
  scala.io.StdIn.readLine()
  
}