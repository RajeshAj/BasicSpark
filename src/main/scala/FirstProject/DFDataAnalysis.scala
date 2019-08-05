package FirstProject


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, desc, from_unixtime, max, min, to_date, year,explode,split}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}


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

    val rating = fstspk.read
      .option("header","true")
      .option("inferschema","true")
      .csv("c:\\ml-latest-small/ratings.csv")

    val tagsch = StructType(
      StructField("userId",IntegerType,nullable = false)::
      StructField("movieId",IntegerType,nullable = true)::
      StructField("tag",StringType,nullable = true)::
      StructField("Tag_timestamp",LongType,nullable = true)::Nil)

    val taging = fstspk.read
      .option("header","true")
      //.option("inferschema","true")
      .schema(tagsch)
      .csv("c:\\ml-latest-small/tags.csv")

    //Adding Date column -> Convert Timestamp to date
    val taging_date = taging.select("userId","movieId","tag","Tag_timestamp")
      .withColumn("Tag_Date",from_unixtime(taging.col("Tag_timestamp").divide(1000)))
    taging_date.select(year(to_date(taging_date("Tag_Date"))).alias("year")).show(10)

  // Joining Movie and Rating Data
    val movierating = movie.join(rating, movie("movieId") === rating("movieId"),"left")
      .drop(rating("movieId"))

    // Joining Movie, Rating and tags Data
    val mvt = movierating.join(taging_date,movierating("userId") === taging_date("userId")
      && movierating("movieId") === taging_date("movieId"),joinType = "left")
      .drop(taging_date("userId") ).drop(taging_date("movieId"))

    val dramagre = movie.select("movieid","genres").filter(movie("genres") like "%Drama%").count()
    println(dramagre)


    println(movierating.select("movieId").distinct().count())
    val uniqrating = {
      movierating.select("movieId").filter(movierating("userID") isNotNull).distinct()
    }
    println(uniqrating.count())

    val ratingUS = movierating.select("userID","rating").groupBy("userID").count()
    val order = ratingUS.orderBy(desc("count"))
    order.show(1)

    val agr = movierating.select("title","rating")
      .groupBy("title")
      .agg(min("rating").alias("MIN"),max("rating").alias("MAX"),avg("rating").alias("AVG"))
    agr.show()

    println("users that have rated a movie but not tagged")
      mvt.select("userId").filter(mvt("rating").isNotNull && mvt("tag").isNull).distinct().show()
    println("users that have rated a movie and tagged a movie")
      mvt.select("userId").filter(mvt("rating").isNotNull && mvt("tag").isNotNull).distinct().show()

    val mvty = mvt.select("*")
        .withColumn("Tag_Year",year(to_date(mvt("Tag_Date"))))

    // Shows Year wise movie
    println("Year Wise Movie List",
      mvty.select("movieId","title","Tag_Year")
              .orderBy(desc("Tag_Year")).show())

    mvty.select("movieId","Tag_Year","genres")
      .withColumn("genres", explode(split(mvt("genres"), "[|]")))
      .filter(mvty("Tag_Year").isNotNull)
      .groupBy("genres","Tag_Year").count().distinct()
      .show(30)

  }
}
