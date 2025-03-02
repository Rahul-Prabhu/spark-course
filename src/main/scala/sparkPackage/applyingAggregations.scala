package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object applyingAggregations {
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
    
    read_csv_df.printSchema()
    
    val df = read_csv_df.select("CustLocation", "CustAccountBalance", "TransactionID")
    
    val df_agg = df.groupBy("CustLocation")
    .agg(
        sum(col("CustAccountBalance")).alias("total_account_balance"), 
        count(col("TransactionID")).alias("total_transactions"),
        max(col("CustAccountBalance")).alias("Max_Balance"),
        min(col("CustAccountBalance")).alias("Min_Balance"),
        avg(col("CustAccountBalance")).alias("Avg_Balance")
            )
    
    df_agg.show()
  }
}