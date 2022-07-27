import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import java.sql.Timestamp

object Practice08_Order_Ex9_ExlicitSchema extends App{
  case class Orders(order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
   val ordersSchema = StructType(List(
      StructField("order_id",IntegerType),
     StructField("order_date",TimestampType),
     StructField("order_customer_id",IntegerType),
     StructField("order_status",StringType)
     ))
  //val orderSchemaDDL = "order_id Int,order_date String ,order_customer_id Int,order_status String"
  
  val orderDf = spark.read
  .format("csv")
  .option("header", true)
  .schema(ordersSchema)
  .option("path","/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/orders-201019-002101.csv")
  .load
  import spark.implicits._
  val dataSet = orderDf.as[Orders]  
orderDf.printSchema()
     
  orderDf.show()
  spark.stop()
  
}