package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object cassandra_read {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("SparkHbaseIntegration")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("org.apache.spark.sql.cassandra")
    .options(Map("table"->"country", "keyspace"->"practise"))
    .load()
    
    df.show(false)
  }
}