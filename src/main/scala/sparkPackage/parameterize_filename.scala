package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

object parameterize_filename {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("bank_trans")
      .setMaster("local[*]")
      .set("spark.driver.host", "localhost")
      
      val sc = new SparkContext(conf)
      sc.setLogLevel("Error")
      
      val spark = SparkSession.builder().config(conf).getOrCreate()
      import spark.implicits._
      
      val read_csv_df = spark.read.format("csv").option("header", "true").load(args(0))
      
//      read_csv_df.show()
      read_csv_df.write.format("parquet")
      .option("header", "true")
      .mode("overwrite")
      .save(args(1))
      
      read_csv_df.write.format("com.databricks.spark.avro").option("header", "true")
      .mode("overwrite")
      .save(args(2))
      
    }
}