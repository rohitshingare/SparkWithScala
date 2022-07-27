import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.SaveMode

object Practice05_UnstructToStruct extends App {
  
  val myregex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  
  case class Orders(order_id:Int,customer_id:Int,order_status:String)
  def parser(line: String) = {
    line match{
      case myregex(order_id,date,customer_id,order_status) =>
        Orders(order_id.toInt,customer_id.toInt,order_status)
    }
  }
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Unstructed to Stuctured application")
  sparkConf.set("spark.master", "local[*]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  import spark.implicits._
val lines = spark.sparkContext.textFile("/Users/DELL/Desktop/BigDataTrendyTech/Week-12/DataSet/order_new.csv")
  val order_new = lines.map(parser).toDS().cache()
  
  order_new.printSchema()
  order_new.select("order_id").show
  order_new.groupBy("order_status").count.show()
  spark.stop()
}