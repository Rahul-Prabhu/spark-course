package sparkPackage
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object obj_Dstream {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DStream").setMaster("local[*]")
    .set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val ssc = new StreamingContext(conf, Seconds(2))
    val stream = ssc.textFileStream("file:///C:/data/nifi/nifi_output")
    stream.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}