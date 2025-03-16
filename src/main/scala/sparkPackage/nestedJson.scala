package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object nestedJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("FromJsonDemo")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val df1 = spark.read.format("json").option("multiLine", "true").load("file:///C:/data/spark/address.json")
    
    df1.printSchema()
    df1.show()
    
    val df2 = df1.select("Employee", "orgname", "address.permanent_address", "address.temporary_address")
    
    df2.printSchema()
    df2.show()
    
    val writedf = df2.select(col("Employee"), col("orgname"),
        struct(col("permanent_address"), col("temporary_address")).alias("address")
        
        )
    writedf.printSchema
    writedf.show()
    
    writedf.write.format("json").save("file:///C:/data/spark/employee")
  }
}