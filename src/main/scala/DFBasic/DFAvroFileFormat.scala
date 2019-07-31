package DFBasic


import java.util.Calendar

import org.apache.spark.sql.SparkSession

object DFAvroFileFormat {
  def main(args: Array[String]): Unit = {

    val sparkavro = SparkSession.builder()
      .appName("Read Avro File")
      .master("local")
      .getOrCreate()

    val dfavro = sparkavro.read
      .format("com.databricks.spark.avro")
      .load("c:\\Testavro.avro")

    dfavro.printSchema()
    dfavro.show(10)
    println(dfavro.count())

    val now = Calendar.getInstance()
    val currentMinute = now.get(Calendar.MINUTE)
    println(currentMinute)
    dfavro.write.parquet(s"c:\\output/minute=$currentMinute")

    //dfavro.write.format("com.databricks.spark.avro").save("C:\\output2")

   /* val dfpartest = sparkavro.read.parquet("c:\\output/part-00000-42505225-296f-462f-a870-a40d7e72f85b.snappy.parquet")
    dfpartest.printSchema()
    println(dfpartest.count())
    dfpartest.show(10)*/

  }

}
