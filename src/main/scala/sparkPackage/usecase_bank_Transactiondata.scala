package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object usecase_bank_Transactiondata {
  /**
   * Identify the record with the highest CustAccountBalance
   * In which location we have most number of transactions
   * Which location has the highest sum of total_transaction_amount
   **/
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
    read_csv_df.show()
    
    val use_case1_df = read_csv_df.select("CustAccountBalance", "TransactionID", "CustomerID", "CustLocation").withColumn("CustAccountBalance", col("CustAccountBalance").cast("double"))
    val cols = List("CustAccountBalance", "TransactionID", "CustomerID", "CustLocation")
    val windowSpec = Window.partitionBy(col("CustLocation"))
    
    println("*********************Record with highest bank balance based on location***********************************")
    val result_df = use_case1_df.select(
        col("CustAccountBalance"), 
        col("TransactionID"), 
        col("CustomerID"),
        col("CustLocation"),
        max(
            col("CustAccountBalance")
            ).over(windowSpec).alias("higest_CustAccountBalance")
        )
    
    result_df.dropDuplicates("CustLocation").show(500)
    
    println("**********************Most number of Transactions*******************")
    val usecase2_df = read_csv_df.select(
        col("CustAccountBalance"), 
        col("TransactionID"), 
        col("CustomerID"),
        col("CustLocation")
        ).groupBy("CustLocation").agg(
            count(col("TransactionID")).alias("count_transaction_id")
            ).withColumn("id", lit("1").cast("integer"))
            .withColumn("max_count", 
                max(col("count_transaction_id")).over(Window.partitionBy("id"))
                )
    usecase2_df.filter("max_count=count_transaction_id").show()
    
    println("**********************Higest sum of total_transaction_amount************************")
    val usecase3_df = read_csv_df.select(
        col("CustAccountBalance"), 
        col("TransactionID"), 
        col("CustomerID"),
        col("CustLocation"),
        col("TransactionAmount (INR)")
        ).withColumn("total_transaction_amount", 
            sum(col("TransactionAmount (INR)")).over(windowSpec)
            ).withColumn("id", lit("1").cast("integer"))
            .withColumn("max_sum",
                max(col("total_transaction_amount")).over(Window.partitionBy("id"))
                )
    usecase3_df.filter("max_sum=total_transaction_amount").select("CustLocation", "max_sum").distinct().show()
            
  }
}