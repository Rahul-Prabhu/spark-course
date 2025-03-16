package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
import org.apache.spark.sql.Column
import org.apache.spark.sql.execution.datasources.hbase._

object sparkHbaseIntegration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("SparkHbaseIntegration")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    def catalog_1=s"""{
      "table":{"namespace":"hrdata","name":"employee1"},
      "rowkey":"rowkey",
      "columns":{
        "rowid":{"cf":"rowkey","col":"rowkey","type":"string"},
        "id":{"cf":"cf_emp","col":"eid","type":"string"},
        "name":{"cf":"cf_emp","col":"ename","type":"string"}
      }
      }""".stripMargin
    val df = spark.read.options(Map(HBaseTableCatalog.tableCatalog->catalog_1))
    .format("org.apache.spark.sql.execution.datasources.hbase").load()
    
    df.show()
  }
}