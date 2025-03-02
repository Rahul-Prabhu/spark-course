package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object obj_jsonMultipleLines {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReadJsonFiles")
    .setMaster("local[*]")
    .set("spark.driver.host", "localhost")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    println("*********************Read JSON files*********************")
    val read_df = spark.read.format("json").load("file:///C:/data/json_files/user.json")
    read_df.printSchema()
    read_df.show()
    
    println("*******************Read complex json file*****************")
    //For complex nested json files please provide multiline option as true.
    val read_df_json = spark.read.format("json")
    .option("multiline", "true")
    .load("file:///C:/data/json_files/random_user.json")
    
    read_df_json.printSchema()
    read_df_json.show()
  }
}