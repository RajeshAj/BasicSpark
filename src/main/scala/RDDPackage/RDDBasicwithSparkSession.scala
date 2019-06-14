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
  }

}
