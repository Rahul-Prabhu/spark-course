package sparkPackage
 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
 
 
object sparkWithKafka {
  
  def main(args:Array[String]):Unit={
 
   val conf=new SparkConf().setAppName("File_Streamig").setMaster("local[*]").set("spark.driver.host", "localhost")
   val sc=new SparkContext(conf)
   sc.setLogLevel("Error")
   
   val spark=SparkSession.builder().getOrCreate()
   import spark.implicits._
   
   val readkafka=spark.readStream.format("kafka")
                 .option("kafka.bootstrap.servers", "localhost:9092")
                 .option("subscribe", "read_topic")
                 .load()
                 .withColumn("value", expr("cast(value as string)"))
                 .selectExpr("concat(value,',Hello') as value")
                 
   //write to kafkatopic
                 
    readkafka.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
              .option("topic", "write_topic")
              .option("checkpointLocation", "file:///C:/data/streaming/check_point")
              .start()
              .awaitTermination()
   
  }
}
