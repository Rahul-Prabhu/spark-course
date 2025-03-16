package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source

object flatten_json_webURL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("nestedJson")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val input_data = Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString
    println(List(input_data))
    
    val rdd = sc.parallelize(List(input_data))
    
    val df = spark.read.json(rdd)
    println("************************Raw data*************************")
    df.printSchema
    df.show(false)
    
    println("******************Flatten array*************************")
    val flatten_df = df.withColumn("results", explode(col("results")))
    flatten_df.printSchema
    flatten_df.show(false)
    
    println("******************Flatten struct data********************")
    val flatten_struct_df = flatten_df.select("nationality", "seed", "version",
        "results.user.cell", "results.user.dob", "results.user.email", "results.user.gender",
        "results.user.location.city", "results.user.location.state", "results.user.location.zip", "results.user.location.street"
        )
    flatten_struct_df.show(false)
    flatten_struct_df.printSchema
  }
}