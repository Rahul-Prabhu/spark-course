package sparkPackage
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object objSeamlessDataFrame {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Seamless-DataFrame").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    // Invoke a Spark session
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val read_csv_data = spark.read.format("csv").option("header", "true")
    .option("delimeter", ",")
    .load("file:///C:/data/train.csv")
    
    read_csv_data.printSchema()
    println("**************csv data train employee*******************")
    read_csv_data.show(5)
    
    //Read a pipe delimited file
    println("******************Reading pipe delimited file*******************")
    val read_pipe_df = spark.read.format("csv").option("header", "true")
    .option("delimiter","|").load("C:\\data\\countries_pipe_seperated.txt")
    
    read_pipe_df.show(5)
    
    //Writing to another file - delimited format
    println("***************Writing to another file delimited file format*****************")
    read_pipe_df.write.mode("overwrite")
    .option("delimiter", "~")
    .format("csv")
    .save("file:///C:/data/countries_file")
    
    //Writing to json format
    println("***************Writing to another json file format*****************")
    read_pipe_df.write.mode("overwrite")
    .option("header", "true")
    .format("json")
    .save("file:///C:/data/countries_file_json")
    
    spark.read.format("json").load("file:///C:/data/countries_file_json").show()
  }
}