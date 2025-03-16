package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
import org.apache.spark.sql.Column

object flattenXMLFile {
  
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("flattenXML")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("com.databricks.spark.xml")
    .option("rowTag", "catalog_item")
    .load("file:///C:/data/product.xml")
    
    println("**********************raw data******************************")
    df.printSchema
    df.show(false)
    
    println("****************Flattening array**************************")
    val flattenArrayDf = df.withColumn("size", explode(col("size")))
    flattenArrayDf.printSchema
    flattenArrayDf.show(false)
    
    println("***************Flattening Struct******************")
    val flatten_struct_df = flattenArrayDf.select(
        col("_gender") , col("item_number") , col("price") , col("size.*")    
    )
    
    println("*****************Flattening array again********************")
    val df_array = flatten_struct_df.withColumn("color_swatch", explode(col("color_swatch")))
    df_array.printSchema
    df_array.show(false)
    
    println("************************Flattening Struct again****************")
    
    val df_struct = df_array.select(col("_gender"), col("item_number"), col("price"), col("_description"), col("color_swatch._image").alias("image"), col("color_swatch._VALUE").alias("image_value"))
    df_struct.printSchema
    df_struct.show(false)
  }
}

