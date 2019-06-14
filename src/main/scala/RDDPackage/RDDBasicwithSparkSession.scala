package RDDPackage

import org.apache.spark.sql.SparkSession


object RDDBasicwithSparkSession {
  def main(args: Array[String]): Unit = {

    val sparksession = SparkSession.builder()
      .appName("Spark Session Basic")
      .master("local")
      .getOrCreate()

    val array = Array(1,2,3,4,5)
    val rdd1 = sparksession.sparkContext.parallelize(array,2)
    rdd1.collect().foreach(println)

    val filerdd1 = "C:\\Exchange_Summary by Frame._2019_06_13.csv"
    val filerdd2 = sparksession.sparkContext.textFile(filerdd1,3)
    filerdd2.take(2).foreach(println)
    val filerdd3 = filerdd2.map(y => y.split(","))
    //filerdd3.take(2).foreach(println)
    val filerdd4 = filerdd3.filter(x => x(1).contains("Nivea" ))
    println("Count with Match Char",filerdd4.count())
    val filerdd5 = filerdd4.map(line => line(0))
    filerdd5.distinct.collect().sortBy(x=> x).foreach(println)

  }

}
