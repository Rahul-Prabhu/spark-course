package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object obj_XML_file {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("XMLFileIngestion")
    .setMaster("local[*]")
    .set("spark.driver.host", "localhost")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    println("******************Read XML File**************************")
    
    val read_xml = spark.read.format("com.databricks.spark.xml")
    .option("rowTag", "book")
    .load("file:///C:/data/xml_files/books.xml")
    
    read_xml.printSchema()
    read_xml.show(5)
    
    println("***************************Writing xml file******************")
    read_xml.write.format("com.databricks.spark.xml")
    .option("rowTag", "book")
    .mode("overwrite")
    .save("file:///C:/data/xml_files/books")
  }
  
}