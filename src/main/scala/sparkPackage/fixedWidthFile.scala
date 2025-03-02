package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

object fixedWidthFile {
  
  def dml(column: Array[Row]): List[String] = {
    var final_dml:String = ""
    var last_dml:List[String] = Nil
    
    for(i<-column) {
      val data = i.mkString(",")
      
      val columnName = data.split(",")(0)
      val pos = data.split(",")(1)
      val len = data.split(",")(2)
      
      final_dml = s"substring(fixed_data,$pos,$len) as $columnName"
      last_dml = last_dml :+ final_dml
      
    }
    return last_dml
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("bank_trans")
    .setMaster("local[*]")
    .set("spark.driver.host", "localhost")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    
    val schemaFile = spark.read.format("json").load("file:///C:/data/dml.json")
    
    val col_df = schemaFile.collect()
    val dd = dml(col_df)
    
    val inputFile = spark.read.format("csv").option("header", "false")
    .load("file:///C:/data/input.txt").withColumnRenamed("_c0", "fixed_data")
    
    println("***********************Extracting the data**************************")
    val df = inputFile.selectExpr(dd:_*)
    df.show()
  }
}