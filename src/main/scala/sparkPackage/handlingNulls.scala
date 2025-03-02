package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object handlingNulls {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("bank_trans")
    .setMaster("local[*]")
    .set("spark.driver.host", "localhost")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val read_df = spark.read.format("csv").option("header", "true")
    .option("delimiter", "|")
    .load("file:///C:/data/spark/account.txt")
    read_df.show()
    
    read_df.na.drop(how="any", cols=Seq("Bank_ID")).show()
    
    val select_df = read_df.select(
        col("ACCT_ID").cast("Int"), col("Gender"), col("Bank_ID"), col("Country")
        )
    select_df.printSchema()
    select_df.na.fill("NA").na.fill(9999).show()
    
    select_df.withColumn("country", coalesce(col("Country"), lit("Default"))).show()
    
    
  }
}