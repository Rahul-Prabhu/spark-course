package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object sortData {
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
    
    println("****************************Sorting*****************************")
    val sort_df = read_csv_df.orderBy(col("CustGender").desc, col("CustLocation").asc)
    sort_df.show()
    
    val custlocation_df = read_csv_df.orderBy(col("CustLocation").desc_nulls_last)
    custlocation_df.show()
    
    val cust_gender = read_csv_df.select("CustGender").dropDuplicates()
    cust_gender.show()
    
    println("********************Sorting on  Ascending - Default************************")
    cust_gender.orderBy(col("CustGender").asc).show
    
    println("********************Sorting on  Ascending - Nulls first************************")
    cust_gender.orderBy(col("CustGender").asc_nulls_first).show
    
    println("********************Sorting on  Ascending - Nulls last************************")
    cust_gender.orderBy(col("CustGender").asc_nulls_last).show
    
    println("********************Sorting on  Descending - Default************************")
    cust_gender.orderBy(col("CustGender").desc).show
    
    println("********************Sorting on  Descending - Nulls first************************")
    cust_gender.orderBy(col("CustGender").desc_nulls_first).show
    
    println("********************Sorting on  Descending - Nulls last************************")
    cust_gender.orderBy(col("CustGender").desc_nulls_last).show
  }
}