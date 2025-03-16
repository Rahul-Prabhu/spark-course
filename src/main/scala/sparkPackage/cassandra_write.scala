package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object cassandra_write {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("SparkHbaseIntegration")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("csv").option("header", "true").option("delimiter", "|")
    .load("file:///C:/data/countries.csv")
    
//    df.show(false)
    
    df.write.format("org.apache.spark.sql.cassandra")
    .option("spark.cassandra.connection.host", "localhost")
    .option("spark.cassandra.connection.port", "9042")
    .option("keyspace", "practise")
    .option("table", "country")
    .mode("append")
    .save()
  }
}