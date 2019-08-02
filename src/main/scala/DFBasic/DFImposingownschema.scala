package DFBasic

//import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object DFImposingownschema {
  def main(args: Array[String]): Unit = {
    val sparksch =  SparkSession.builder()
      .appName("imposing own schema")
      .master("local")
      .getOrCreate()

    val file1 = sparksch.read
      .option("header", "True")
      .option("inferschema","true")
      .csv("c:\\TEST.csv")
    file1.printSchema()
    file1.show(5)

    val sch = StructType(
      StructField ("Deal",StringType,nullable = true)::
        StructField("Brand",StringType,nullable = true)::
    StructField("Channel",StringType,nullable = true)::Nil)

    val file2 = sparksch.read.option("Header","true").schema(sch).csv("c:\\TEST.csv")
    file2.printSchema()
    file2.show(5)
   }

}
