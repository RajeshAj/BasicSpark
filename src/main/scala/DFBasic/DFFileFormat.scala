package DFBasic

import org.apache.spark.sql.SparkSession

object DFFileFormat {
  def main(args: Array[String]): Unit = {

    val spark =  SparkSession.builder()
      .master("local")
      .appName("DF File Format")
      .getOrCreate()

    val dfjson = spark.read.json("C:\\TESTJson.json")
    dfjson.printSchema()
    println(dfjson.count())
    dfjson.show(5)

    val dfparq = spark.read.parquet(path="c:\\TESTParq.parquet")
    dfparq.printSchema()
    println(dfparq.count())
    dfparq.show(10)

    val dforc = spark.read.orc("c:\\Testorc.orc")
    dforc.printSchema()
    println(dforc.count())
    dforc.show(10)


  }
}
