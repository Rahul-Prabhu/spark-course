package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object workingWithDates {
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
    
    read_csv_df.select("CustomerDOB")
    .withColumn("current_date", current_date)
    .withColumn("date_formatted", date_format(col("current_date"), "MM-dd-yyyy"))
    .withColumn("cust_dob_format", date_format(to_date(col("CustomerDOB"), "dd-MM-yyyy"), "MM-dd-yyyy"))
    .withColumn("dob_plust_month", add_months(col("current_date"), 1))
    .withColumn("dob_plus_day", date_add(col("current_date"), 1))
    .withColumn("age", datediff(col("current_date"), to_date(col("CustomerDOB"), "dd-MM-yyyy")))
    .withColumn("current_year", year(col("current_date")))
    .withColumn("current_month", year(col("current_date")))
    .withColumn("current_quarter", quarter(col("current_date")))
    .show()
    
  }
}