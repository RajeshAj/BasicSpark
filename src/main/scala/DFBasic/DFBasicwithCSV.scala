package DFBasic

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DFBasicwithCSV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("DF with CSV")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)
    val df = sql.read
        .option("header",true)
        .option("inferSchema",true)
        .format("com.databricks.spark.csv")
        .load("C:\\TEST.csv")
    df.printSchema()
    df.show(10)

  }
}
