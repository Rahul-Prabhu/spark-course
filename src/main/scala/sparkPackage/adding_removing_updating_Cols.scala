package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object adding_removing_updating_Cols {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("depl")
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
    
    // Select required columns
    val select_df = read_csv_df.select(
        col("TransactionID"), col("CustomerID"), col("CustGender"),
        col("CustAccountBalance"), col("TransactionDate"), 
        col("TransactionAmount (INR)").alias("TransactionAmount")
        )
    
    select_df.show()
    
    println("*************************Selecting columns using selectExpr********************")
    val select_df_expr = select_df.selectExpr(
        "TransactionID", "CustomerID", "CustGender",
        "CustAccountBalance", "TransactionDate",
        "TransactionAmount", 
        "concat('20', split(TransactionDate, '/')[2]) as TransactionYear"
    )
    select_df_expr.show
    
    println("***********************Selecting columns using with***********************")
    
    val with_df = select_df
                  .withColumn("TransactionYear", expr("concat('20', split(TransactionDate, '/')[2])"))
                  .withColumn(
                      "TransactionDate_Format", 
                      date_format(
                          to_date(col("TransactionDate"), "dd/MM/yy"), 
                          "MM-dd-YY"
                          )
                   )
                  .withColumnRenamed("TransactionDate_Format", "Trans_Date")
                  .withColumn("user_id", lit("John"))
                  .withColumn("created_timesamp", current_timestamp())
                  .withColumn("create_dt", current_date)
    
    
    with_df.show(false)
    
    val drop_col_df = with_df.drop("TransactionDate", "Transaction_Amount")
    .withColumn("Gender", expr("case when custGender='M' then 'Male'" +
                    "when custGender='F' then 'Female'" +
                    "when custGender='T' then 'Transgender'" + 
                    "else 'Unknown' end"
    ))
    
    val fil_col = drop_col_df.filter(col("Gender").isin("Transgender"))
    fil_col.show()
  }
}