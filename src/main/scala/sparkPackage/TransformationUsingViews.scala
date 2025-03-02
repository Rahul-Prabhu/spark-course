package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TransformationUsingViews {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("depl")
    .setMaster("local[*]")
    .set("spark.driver.host", "localhost")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("csv").option("header", "true")
    .load("file:///C:/data/train.csv")
    
    df.printSchema()
    df.show()
    
    df.persist()
    
    df.createOrReplaceTempView("employee")
    spark.sql(
        "select Age,Businesstravel,department,educationfield,monthlyincome,jobrole,gender from employee where age>45 and educationfield='Life Sciences' and JobRole != 'Sales Executive'"
        ).show()
        
    //Using DSL
    println("***************************DSL**********************")
    val read_csv_df = df
    .select(col("age"), col("businesstravel"), col("educationfield"), col("jobrole"), col("gender"))
    .filter(
        col("age")>45 
        && col("educationfield")==="Life Sciences" 
//        && col("jobrole") =!="Sales Executive"  
        && col("jobrole").isin("Research Scientist", "Manager")
        )
    
    read_csv_df.show()
  }
}