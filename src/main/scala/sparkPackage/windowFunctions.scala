package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object windowFunctions {
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
    
    val windowSpec = Window.partitionBy("CustomerID").orderBy("CustAccountBalance")
    
    read_csv_df
    .select(col("CustomerID"), col("CustAccountBalance").cast("double"))
    .withColumn("row_num", row_number().over(windowSpec))
    .withColumn("rank", rank().over(windowSpec))
    .withColumn("dense_rank", dense_rank().over(windowSpec))
//    .where(col("dense_rank") =!= col("rank"))
    .withColumn("percent_rank", percent_rank().over(windowSpec))
    .withColumn("lag", lag("CustAccountBalance", 1).over(windowSpec))
    .withColumn("lead", lead("CustAccountBalance", 1).over(windowSpec))
    
    .withColumn("avg_acct_bal", avg(col("CustAccountBalance")).over(windowSpec))
    .withColumn("max_acct_bal", max(col("CustAccountBalance")).over(windowSpec))
    .withColumn("min_acct_bal", min(col("CustAccountBalance")).over(windowSpec))
    .where(col("CustomerID") === "C1119249")
    .show()
    
    
  }
}