package DFBasic

import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ StringType, StructField, StructType}

object DFBasicwithSparkSession {
  def main(args: Array[String]): Unit = {
     val sparksess = new sql.SparkSession.Builder()
       .appName("DFSparkSession")
       .master("local")
       .getOrCreate()
    val rdd = sparksess.sparkContext.parallelize(Array("rajesh","spark","first","learning"))
    val schema = StructType(
      StructField("ID as String", StringType ,true):: Nil
    )
    val rowrdd = rdd.map(element => Row(element))
    val df = sparksess.createDataFrame(rowrdd,schema)
    df.printSchema()
    df.show()
    df.show(3)
  }

}
