package DFBasic

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
    // Create RDD based on the Arrary Value
    val rdd = sc.parallelize(Array(1,2,3,4,5))
    // Create a Schema for the table
    val schema  = StructType(StructField ("ID",IntegerType,true):: Nil)
    // change the Array value in a row
     val rdd1 = rdd.map(line => Row(line))
    rdd1.collect().foreach(println)
    // Load the data into the schema that we designed
    val df = sqlsc.createDataFrame(rdd1,schema)
    // Print the Schema structure
    df.printSchema()
    // Print the DF
    df.show()



  }
}
