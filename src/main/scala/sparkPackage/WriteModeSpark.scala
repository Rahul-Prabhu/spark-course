package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object WriteModeSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WriteModeSpark")
    .setMaster("local[*]")
    .set("spark.driver.host", "localhost")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("json").load("file:///C:/data/json_files/user.json")
    df.write.format("json").mode(SaveMode.Ignore).save("file:///C:/data/json_files/user_json1")
  }
}