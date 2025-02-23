package sparkPackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object rdd_LogData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogDataAnalyzer").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    val rdd_input = sc.textFile("file:///C:/data/Hadoop_2k.log")
    sc.setLogLevel("Error")
    
    println("Total Log count: "+rdd_input.count())
    
    val rdd_warn = rdd_input.filter(x=>x.contains("WARN"))
    val rdd_error = rdd_input.filter(x=>x.contains("ERROR"))
    val rdd_info = rdd_input.filter(x=>x.contains("INFO"))
    
    println(rdd_warn.count())
    println(rdd_error.count())
    println(rdd_info.count())
  }
}