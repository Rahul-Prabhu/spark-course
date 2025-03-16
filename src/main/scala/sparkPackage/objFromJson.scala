package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object objFromJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("FromJsonDemo")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val dml = StructType(Seq(
        StructField("name", StringType, true),
        StructField("age", StringType, true),
        StructField("Address", StringType, true)
      )
    )
    
    val json_schema = StructType(
        Seq(
            StructField("city", StringType, true),
            StructField("state", StringType, true),
            StructField("country", StringType, true)
            )
        )

    val df = spark.read.format("csv")
    .option("delimiter", "~")
    .schema(dml)
    .load("file:///C:/data/file.txt")
    
    df.printSchema
    df.show
    println("****************Flattening json***********************")
    val final_df = df.select(
        col("name"), 
        col("age"), 
        from_json(
            col("address"), json_schema
            ).as("address")
        )
    final_df.printSchema()
    final_df.show
    
    val json_df = final_df.select(col("name"), col("age"), col("address.city"), col("address.state"), col("address.country"))
    json_df.printSchema
    json_df.show
    
    println("********************MultiLine Json*********************************")
    val df1 = spark.read.format("json").load("C:/data/spark/file1.json")
    val df2 = spark.read.format("json").option("multiLine", "true").load("C:/data/spark/file2.json")
    
    df1.show()
    df2.show
    
    
    
  }
    
}