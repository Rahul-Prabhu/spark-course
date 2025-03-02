package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object removeDuplicates {
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
    
    println("Total count "+read_csv_df.count())
    
    val distinctDf = read_csv_df.distinct()
    println("Total distinct count " + distinctDf.count)
    
    println("***************Drop duplicate data based on columns***************")
    val distinct_d = read_csv_df.dropDuplicates()
    println("Total distinct count row "+distinct_d.count())
    
    val distinct_cust_id = read_csv_df.dropDuplicates("CustomerID")
    println("Total distinct customer_id count "+distinct_cust_id.count())
    
    val distinct_gender = read_csv_df.dropDuplicates("CustGender")
    println("Total distinct Gender count "+distinct_gender.count())
    
    distinct_gender.show()
  }
}