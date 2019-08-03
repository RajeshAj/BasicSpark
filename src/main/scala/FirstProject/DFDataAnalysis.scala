package FirstProject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.{min, max,avg}

object DFDataAnalysis {

  def main(args: Array[String]): Unit = {
    val fstspk = SparkSession.builder()
      .appName("movie data analysis")
      .master("local")
      .getOrCreate()

    val movie = fstspk.read
        .option("header","true")
        .option("inferSchema","true")
        .csv("c:\\ml-latest-small/movies.csv")
    //movie.printSchema()
    //movie.show(10)
    //println(movie.count())

    val dramagre = movie.select("movieid","genres").filter(movie("genres") like "%Drama%").count()
    println(dramagre)
    //dramagre.write.csv("c:\\testgenres.csv")

    val rating = fstspk.read
      .option("header","true")
      .option("inferschema","true")
      .csv("c:\\ml-latest-small/ratings.csv")
    rating.printSchema()
    rating.show(10)


    val movierating = movie.join(rating, movie("movieId") === rating("movieId"),"left")
      .drop(rating("movieId"))
    movierating.show(10)


   // movierating.select(("movieid")).show(10)
    println(movierating.select("movieId").distinct().count())
   val uniqrating = movierating.select("movieId").filter(movierating("userID") isNotNull).distinct()
   println(uniqrating.count())

    val ratingUS = movierating.select("userID","rating").groupBy("userID").count()
    val order = ratingUS.orderBy(desc("count"))
    order.show(1)

    val agr = movierating.select("title","rating")
      .groupBy("title")
      .agg(min("rating").alias("MIN"),max("rating").alias("MAX"),avg("rating").alias("AVG"))
    agr.show()
  }
}
