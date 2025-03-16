package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
import org.apache.spark.sql.Column

object FlatteningUsingFunction {
  
  def flattenStructSchema(schema:StructType, prefix:String=null): Array[Column] ={
    schema.fields.flatMap(
        f=>
          {
            val columnName = if(prefix==null) f.name else (prefix+"."+f.name)
            println("************Printing Column Name*********************")
            println(columnName)
            f.dataType match{
              case st: StructType => flattenStructSchema(st,columnName)
              case _=>Array(col(columnName).as(columnName.replace(".", "_")))
            }
          }
        )
  }
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    .setAppName("nestedJson")
    .set("spark.driver.host", "localhost")
    .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val input_data = Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString
    
    val rdd = sc.parallelize(List(input_data))
    
    val df = spark.read.json(rdd)
    println("************************Raw data*************************")
    df.printSchema
    df.show(false)
    
    println("******************Flatten array*************************")
    val flatten_df = df.withColumn("results", explode(col("results")))
    flatten_df.printSchema
    flatten_df.show(false)
    
    println(flatten_df.schema)
    
    flattenStructSchema(flatten_df.schema)
    
    val web_api_flat = flatten_df.select(flattenStructSchema(flatten_df.schema):_*)
    web_api_flat.printSchema
    web_api_flat.show(false)
    
    flatten_df.schema.fields.foreach(f=>println(f.name+","+f.dataType))
    
    
  }
}