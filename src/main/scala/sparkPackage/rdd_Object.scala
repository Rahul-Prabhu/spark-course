package sparkPackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object rdd_Object {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BigBasketJob").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)

    sc.setLogLevel("Error")

    val data = sc.textFile("file:///home/cloudera/data/BigBasket.csv")
    val filter_category = data.filter(x=>x.contains("Beauty"))
    val filter_subcategory = filter_category.filter(x=>x.contains("Skin Care"))
    
    filter_subcategory.take(5).foreach(println)
    
    filter_subcategory.coalesce(1).saveAsTextFile("file:///home/cloudera/data/big_basket")

  }
}