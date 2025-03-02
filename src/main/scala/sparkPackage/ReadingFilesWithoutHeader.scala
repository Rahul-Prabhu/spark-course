package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ReadingFilesWithoutHeader {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("depl")
    .setMaster("local[*]")
    .set("spark.driver.host", "localhost")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val dml = StructType(Array(
          StructField("state", StringType, true),
          StructField("capital", StringType, true),
          StructField("language", StringType, true),
          StructField("country", StringType, true)
          )
        )
    
    val df = spark.read.format("csv").option("header", "false").schema(dml)
    .load("file:///C:/data/countries/countries*")
    
    df.show(100)
    
    df.coalesce(1).write.mode("overwrite").format("csv").partitionBy("country")
    .save("file:///C:/data/countries/country_write")
    
    df.coalesce(1).write.mode("overwrite").format("csv").partitionBy("country", "language")
    .save("file:///C:/data/countries/country_write_subpartition")
  }
}