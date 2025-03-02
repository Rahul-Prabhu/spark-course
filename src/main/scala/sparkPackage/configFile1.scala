package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

object configFile1 {
  def main(args: Array[String]): Unit = {
    ConfigFactory.invalidateCaches()
    println("*****************Application.conf****************")
    
    val config = ConfigFactory.load("Application.conf").getConfig("MyProject")
    println(config.getString("spark.app-name"))
    println(config.getString("spark.master"))
    println(config.getString("spark.log-level"))
    
    println(config.getString("mysql.username"))
    println(config.getString("mysql.password"))
    println(config.getString("mysql.url"))
    
    val conf = new SparkConf().setAppName(config.getString("spark.app-name"))
      .setMaster(config.getString("spark.master"))
      .set("spark.driver.host", "localhost")
      
      val sc = new SparkContext(conf)
      sc.setLogLevel(config.getString("spark.log-level"))
      
      val spark = SparkSession.builder().config(conf).getOrCreate()
      import spark.implicits._

    
    println("************Application properties*************************")
    val config1 = ConfigFactory.load("Application.properties")
    println(config1.getString("dev.input.base.dir"))
    println(config1.getString("dev.output.base.dir"))
    
    println(config1.getString("prod.input.base.dir"))
    println(config1.getString("prod.output.base.dir"))
    
    println(config1.getString("input"))
    println(config1.getString("output"))
    
    if (args(0) == "dev")
    {
      //dev
      val read_csv_df = spark.read.format("csv").option("header", "true")
      .load(config1.getString("dev.input.base.dir"))
      
      read_csv_df.write.mode("overwrite").save(config1.getString("dev.output.base.dir"))
    }
    
    else 
    {
      //prod
      val read_csv_df = spark.read.format("csv").option("header", "true")
      .load(config1.getString("prod.input.base.dir"))
      
      read_csv_df.write.mode("overwrite").save(config1.getString("prod.output.base.dir"))
    }

    
  }
}