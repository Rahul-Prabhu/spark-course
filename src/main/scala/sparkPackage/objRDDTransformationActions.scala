package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object objRDDTransformationActions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogDataAnalyzer").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val rdd_input = sc.textFile("file:///C:/data/countries.csv")
    val rdd_india = rdd_input.filter(x=>x.contains("India"))
    rdd_india.foreach(println)
    println(rdd_india)
    rdd_india.collect.foreach(println)
    
    rdd_input.intersection(rdd_india).foreach(println)
    rdd_input.subtract(rdd_india).foreach(println)
    
    val rdd = sc.parallelize(List(1,2,3,4,5))
    rdd.foreach(println)
    println(rdd.reduce(_+_))
  }
}