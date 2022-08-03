import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TopMovieRatedProblem extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "topMovieRated")

  val input = sc.textFile("/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/ratings-201019-002101.dat")
  
  val mappedRdd = input.map(x => {
    val fields = x.split("::")
    (fields(1),fields(2))
  })
  
  //input
  //(1193,5)
  //(1193,3)
  //(1193,4)
  
  //output
  ////(1193,(5.0,1.0))
  //(1193,(3.0,1.0))
  //(1193,(4.0,1.0))
  
  val newMapped = mappedRdd.mapValues(x => (x.toFloat,1.0))
   //input
  ////(1193,(5.0,1.0))
  //(1193,(3.0,1.0))
  //(1193,(4.0,1.0))
  
  //output
  //(1193,(12,3.0))
  
  val reduceRdd = newMapped.reduceByKey((x,y) => (x._1+ y._1, x._2 + y._2))
  
  //input
  //(1193,(12.0,3.0))
  val filterRdd = reduceRdd.filter(x => x._2._2 >1000)
  //input
  //(1193,(12000.0,3000.0))
  
  //output
  //(1193,4)
  
  val ratingsProcessed = filterRdd.mapValues(x => x._1/x._2).filter(x => x._2 > 4.0)
  
  //ratingsProcessed.collect().foreach(println)
  
  
  val moviesRdd = sc.textFile("/Users/DELL/Desktop/BigDataTrendyTech/Week-11/DataSet/movies-201019-002101.dat")
  
 val moviesMapeed = moviesRdd.map(x => {
    val fields = x.split("::")
    (fields(0),fields(1))
  })
  
  val joinedRdd = moviesMapeed.join(ratingsProcessed)
  
  val topMovies = joinedRdd.map(x => x._2._1)
  
  topMovies.collect.foreach(println)

}