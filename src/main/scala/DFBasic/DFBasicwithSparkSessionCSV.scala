package DFBasic

import org.apache.spark.sql.SparkSession

object DFBasicwithSparkSessionCSV {
 def main (args: Array[String]):Unit = {

  val spark = SparkSession.builder()
    .appName("Spark Session with CSV")
    .master("local")
    .getOrCreate()
  val property = Map("header" -> "true","inferSchema" -> "true")
  val df = spark.read
    //  .option("header",true).option("inferSchema",true)
    //.options(Map("header" -> "true","inferSchema" -> "true"))
      .options(property)
    .csv("C:\\TEST.csv")
  df.printSchema()
  df.show(10)

 }
}
