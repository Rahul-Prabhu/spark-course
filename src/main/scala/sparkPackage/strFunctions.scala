package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object strFunctions {
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
    read_csv_df.persist()
    read_csv_df.show()
    
    val trans_df = read_csv_df.withColumn(
        "Unique_Id", concat_ws("-", col("TransactionID"), col("CustomerID"))
        ).withColumn("Loc_Position", instr(col("CustLocation"), "A"))
        .withColumn("cust_loc_length", length(col("CustLocation")))
        .withColumn("lower_cust_loc", lower(col("CustLocation")))
        .withColumn("upper_cust_loc", upper(col("CustLocation")))
        .withColumn("Repeat_Gender", repeat(col("CustGender"), 2))
        .withColumn("Cust_Location_First3", substring(col("CustLocation"), 2, 3))
        .withColumn("custLoc_Replace", regexp_replace(col("CustLocation"), "BAI", "BAY"))
        .withColumn("TransAm_lpad", lpad(col("TransactionAmount (INR)"), 10, "0"))
        .withColumn("TransAm_rpad", rpad(col("TransactionAmount (INR)"), 10, "0"))
        .withColumn("CustLocation_lpad", lpad(col("CustLocation"), 10, "0"))
    trans_df.show()
    trans_df.printSchema()
  }
}