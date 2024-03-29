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

    val file = "C:\\TEST.csv"
    val filerdd1 = sc.textFile(file,5)
    println("No of Rows",filerdd1.count())
    filerdd1.take(10).foreach(println)
    val filerdd2 = filerdd1.map(line => line.split(","))
    val filerdd3 = filerdd2.map(line => (line(0),line(1)))
    filerdd3.take(10).foreach(println)
  }

}
