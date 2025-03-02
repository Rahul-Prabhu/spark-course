package sparkPackage
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object obj_Avro_file {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Seamless-DataFrame").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    // Invoke a Spark session
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val read_df = spark.read.format("csv").option("header", "true")
    .option("delimeter", ",")
    .load("file:///C:/data/train.csv")
    
    read_df.write.format("com.databricks.spark.avro").option("header", "true")
    .mode("overwrite").save("file:///C:/data/example/avro_files")
    
    println("********************Reading Avro Data************************")
    val read_avro = spark.read.format("com.databricks.spark.avro").option("header", "true")
    .load("file:///C:/data/example/avro_files")
    read_avro.show(5)

  }
}