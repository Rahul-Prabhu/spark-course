package sparkPackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object sparkFileStreamingStructure {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("File_Streaming").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val dml = StructType(Array(
        StructField("id", StringType, true),
        StructField("name", StringType, true))
        );
    
    val df = spark.readStream.format("csv").schema(dml).load("file:///C:/data/streaming/src_data")
    
    df.writeStream.format("console")
    .option("checkpointLocation", "file:///C:/data/streaming/check_point")
    .start().awaitTermination()
    
  }
}