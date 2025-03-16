package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
import org.apache.spark.sql.Column
import org.apache.spark.sql.execution.datasources.hbase._

object hbase_write {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("SparkHbaseIntegration")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    //read input file
    val df = spark.read.format("csv").option("header", "true").load("/user/cloudera/datasets/dev/input/account.csv")
    
    val sel_df = df.select("Age", "BusinessTravel", "DailyRate")
    
    //define catalog.Map spark cols to hbase cols
    def catalog_write = s"""
      "table": {"namespace": "hbase_practise", "name": "hbase_test"},
      "rowkey": "rowkey",
      "columns": {
      "Age": {"cf":"rowkey","col":"rowkey","type":"string"},
      "BusinessTravel": {"cf": "cf_test", "col": "bt", "type": "string"},
      "DailyRate": {"cf": "cf_test", "col": "dt", "type": "string"}
      }
      """.stripMargin
      
    val write_df = sel_df.write.options(
        Map(HBaseTableCatalog.tableCatalog->catalog_write, HBaseTableCatalog.newTable->"4"))
        .format("org.apache.spark.sql.execution.datasources.hbase").save()
        
  }
}