package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object nested_json_struct_array {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("nestedJson")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val df1 = spark.read.format("json").option("multiLine", "true")
    .load("file:///C:/data/spark/cake.json")
    
    df1.printSchema()
    println("**********************Raw data********************************")
    df1.show(false)
    
    println("*********************Flatten array data************************")
    val flatten_array_df = df1.withColumn("topping", explode(col("topping")))
    flatten_array_df.printSchema
    flatten_array_df.show(false)
    
    println("*********************Flatten Struct data************************")
    val struct_data_df = flatten_array_df.withColumn("topping_id", col("topping.id"))
    .withColumn("topping_type", col("topping.type"))
    
    struct_data_df.printSchema
    struct_data_df.show(false)
    
    val select_df = struct_data_df.select(
          col("id"), col("name"), col("ppu"), col("type"), col("topping_id"), col("topping_type"),
          col("batters.*")
    )
    select_df.printSchema
    select_df.show(false)
    
    println("*********************Flatten array struct************************")
    val flatten_data_df = select_df.select(col("id"), col("type"), col("name"), col("ppu"),
        col("topping_id"), col("topping_type"), explode(col("batter")).alias("batter_flatten")
        )
    flatten_data_df.printSchema
    flatten_data_df.show(false)
    
    println("******************final array **************************")
    val final_df = flatten_data_df.select(
        col("id"), col("name"), col("ppu"), col("type"), col("topping_id"), 
        col("topping_type"),col("batter_flatten.id").as("batter_id"), 
        col("batter_flatten.type").as("batter_type")
        )
    final_df.printSchema
    final_df.show(false)
  }
}