import org.apache.log4j.Level 

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object Assignment_Problem1 extends App {
  //creating sparkConf object
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Assignment windowdata")
  sparkConf.set("spark.master","local[2]")
  
  //Step1 -creating a spark session
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  //Step 2 -Setting the logging level to Error
 Logger.getLogger("org").setLevel(Level.ERROR)
  
 //Step 3 Explicit schema definition programmatically using StructType 
 val windowdataSchema = StructType(List(
     StructField("Country",StringType),
     StructField("weeknum",IntegerType),
     StructField("numinvoices",IntegerType),
     StructField("totalquantity",IntegerType),
     StructField("invoicevalue",DoubleType)))
     
 //Step 3 contd.. Loading the file and creation of dataframe using dataframe reader API, using explicitly specified schema
     val windowdataDF = spark.read.format("csv").schema(windowdataSchema)
     .option("path", "/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/windowdata-201021-002706.csv")
     .load()
     //print first 20 records of the dataframe
     windowdataDF.show()
     
     //Step 4: Saving the data in Parquet format using Dataframe Writer API//Data is two-level partitioned on Country and weeknum column , these columns have low cardinality //Default output format is parquet/* 
     windowdataDF.write.partitionBy("Country", "weeknum")
     .mode(SaveMode.Overwrite)
     .option("path", "/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/windowdata_output").save()
}



