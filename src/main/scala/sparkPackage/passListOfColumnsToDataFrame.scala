package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object passListOfColumnsToDataFrame {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("bank_trans")
    .setMaster("local[*]")
    .set("spark.driver.host", "localhost")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val read_csv_df = spark.read.format("csv")
    .option("header", "true")
    .load("file:///C:/data/bank_transactions.csv")
    
    read_csv_df.show()
    read_csv_df.printSchema()
    
    read_csv_df.select(
        "CustomerID", "TransactionID", "CustAccountBalance", "CustGender"
        ).show()
    
    val list1 = List("CustomerID", "TransactionID", "CustAccountBalance", "CustGender")
    
    val df2 = read_csv_df.select(list1.map(col):_*)
    
    df2.show()
  }
}