package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
import org.apache.spark.sql.Column

object complex_data_json {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("nestedJson")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val inputDf = spark.read.format("json").option("multiLine", "true")
    .load("file:///C:/data/spark/address_flat.json")
    
    inputDf.printSchema()
    inputDf.show()
    
    val compfd_struct = inputDf.select(col("Employee"), col("orgname"),
        struct(col("permanent_address"), col("temporary_address")).alias("address")
        )
    compfd_struct.printSchema
    compfd_struct.show(false)
    
    val df = compfd_struct.groupBy("Employee", "orgname").agg(collect_list("address"))
    df.printSchema
    df.show(false)
    
    df.write.format("json").save("file:///C:/data/spark/address")
  }
}