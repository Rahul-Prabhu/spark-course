package sparkPackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object spark_Kafka_Integration {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_kafka_integ").setMaster("local[*]")
    .set("spark.driver.allowMultipleContexts", "true").set("spark.driver.host", "localhost")
    
    val sc = new SparkContext(conf)
    
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val ssc = new StreamingContext(conf, Seconds(2))
    
    val kparams = Map[String, Object]("bootstrap.servers" -> "localhost:9092",
        "key.deserializer"->classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "new_consumer",
        "auto.offset.reset" -> "earliest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    
    val topics = Array("new_topic")
    
    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent,
        Subscribe[String, String](topics, kparams)).map(x=>x.value())
        
    stream.foreachRDD(x=>
    if(!x.isEmpty())  
    {
      val df = x.toDF().show()
    }
    )
    
    ssc.start()
    ssc.awaitTermination()
  }
}