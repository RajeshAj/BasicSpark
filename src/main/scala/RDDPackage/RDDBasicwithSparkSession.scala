package RDDPackage

import org.apache.spark.sql.SparkSession


object RDDBasicwithSparkSession {
  def main(args: Array[String]): Unit = {

    val sparksession = SparkSession.builder()
      .appName("Spark Session Basic")
      .master("local")
      .getOrCreate()
    // Create RDD based on the Arrary Value
    val array = Array(1,2,3,4,5)
    val rdd1 = sparksession.sparkContext.parallelize(array,2)
    rdd1.collect().foreach(println)

    // Load a CSV file
    val filerdd1 = "C:\\Exchange_Summary by Frame._2019_06_13.csv"
    val filerdd2 = sparksession.sparkContext.textFile(filerdd1,3)
    filerdd2.take(2).foreach(println)

    // Seperater in our case its comma seperator
    val filerdd3 = filerdd2.map(y => y.split(","))
    filerdd3.take(5).foreach(println)
    // select the row that holds the substring "Nivia"
    val filerdd4 = filerdd3.filter(x => x(1).contains("Nivea" ))
    println("Count with Match Char",filerdd4.count())
    // Select only one based on the sunstring
    val filerdd5 = filerdd4.map(line => line(0))
    // Select only unique value and sort those values
    filerdd5.distinct.collect().sortBy(x=> x).foreach(println)
    // Remove the header from the file
    val filehead = filerdd2.first()
    val filerdd6 = filerdd2.filter(_ != filehead )
    filerdd6.take(10).foreach(println)
  }

}
