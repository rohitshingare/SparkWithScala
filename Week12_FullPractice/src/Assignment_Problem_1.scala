import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window

object Assignment_Problem_1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "append column")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val empDf = spark.read
    .format("json")
    .option("path", "/Users/DELL/Desktop/BigDataTrendyTech/Week-12/Assignment_DataSet/employee-201223-183405.json")
    .load()

  val deptDf = spark.read
    .format("json")
    .option("path", "/Users/DELL/Desktop/BigDataTrendyTech/Week-12/Assignment_DataSet/dept-201223-183405.json")
    .load()
  val joinCondition = empDf.col("deptid") === deptDf.col("deptid")
  val joinType = "left"

  val JoinedDf =
    deptDf.join(empDf, joinCondition, joinType)
      .drop(empDf.col("deptid"))

  val newJoinedDf = JoinedDf.
    groupBy("deptid")
    .agg(count("id").as("count"), first("deptName").as("Dept"))

  newJoinedDf.show

}