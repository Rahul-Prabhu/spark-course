package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object obj_PairRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogDataAnalyzer").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val inputRDD = sc.textFile("file:///C:/data/sales.txt")
    val pairRDD = inputRDD.map(x => (x.split(" ")(0), x.split(" ")(1)))
    val co = pairRDD.collect()
    
    for(i<-co) {
      println(i)
    }
    
    println("****************GroupByKey********************")
    val gr = pairRDD.groupByKey().collect()
    for(i<-gr) {
      println(i)
    }
    
    println("*************ReduceByKey****************")
    val red_by = pairRDD.map{case (key, value) => (key, value.toInt)}.reduceByKey(_+_).collect()
    for(i<-red_by) {
      println(i)
    }
    
    println("***************Map Values**************")
    val map_val = pairRDD.mapValues(a=>(a*2)).collect()
    for(i<-map_val) {
      println(i)
    }
    
    println("**********Keys***************")
    pairRDD.keys.distinct().collect().foreach(println)
    
    println("**********Values***************")
    pairRDD.values.distinct().collect().foreach(println)
    
    println("********************Action on Pair RDD***************")
    pairRDD.countByKey().foreach(println)
  }
}