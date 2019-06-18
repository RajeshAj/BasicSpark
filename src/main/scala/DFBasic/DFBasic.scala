package DFBasic

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object DFBasic {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Basic Datafram")
    val sc = new SparkContext(conf)
    val sqlsc = new SQLContext(sc)

    val rdd = sc.parallelize(Array(1,2,3,4,5))
    val schema  = StructType(StructField ("ID",IntegerType,true):: Nil)
     val rdd1 = rdd.map(line => Row(line))
    rdd1.collect().foreach(println)
    val df = sqlsc.createDataFrame(rdd1,schema)
    df.printSchema()
    df.show()



  }
}
