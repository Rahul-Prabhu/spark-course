package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

object objRowRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RowRDDExample").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val inputFile = sc.textFile("file:///C:/data/countries.txt")
    println("Input file:"+inputFile)
    val inputSplit = inputFile.map(x=>x.split(","))
    
    val inputColumns = inputSplit.map(x=>Row(x(0), x(1), x(2), x(3)))
    
    val fildata = inputColumns.filter(x=>x(2).toString().contains("English"))
    fildata.foreach(println)
    
//    fildata.coalesce(1).saveAsTextFile("file:///C:/data/English_3")
  }
}