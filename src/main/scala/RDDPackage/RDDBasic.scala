package RDDPackage

import org.apache.spark.{SparkConf, SparkContext}

object RDDBasic {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Spark APP")

    val sc = new SparkContext(conf)

    val array = Array(1,2,3,4,5)

    val rdd1 = sc.parallelize(array)
    rdd1.take(2)
    rdd1.collect().foreach(println)
    rdd1.take(3).foreach(println)
    println("Total Count ",rdd1.count())

    val file = "C:\\Exchange_Summary by Frame._2019_06_13.csv"
    val filerdd1 = sc.textFile(file,5)
    println("first line",filerdd1.count())
    filerdd1.take(5).foreach(println)
  }

}
