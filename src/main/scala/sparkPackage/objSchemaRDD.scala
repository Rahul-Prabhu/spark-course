package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class filedml (
  state: String,
  capital: String,
  language: String,
  country: String
)

object objSchemaRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogDataAnalyzer").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val inputFile = sc.textFile("file:///C:/data/countries.csv")
    val inputSplit = inputFile.map(x=>x.split(","))
    
    val inputColumns = inputSplit.map(x=>filedml(x(0), x(1), x(2), x(3)))
    
    val fildata = inputColumns.filter(x=>x.language.contains("English"))
    fildata.foreach(println)
    
    fildata.coalesce(1).saveAsTextFile("file:///C:/data/English")
  }
}