package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

case class filedml1 (
  state: String,
  capital: String,
  language: String,
  country: String
)

object obj_DF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RowRDDExample").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    //invoke a sparkSession
    val spark=SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val inputFile = sc.textFile("file:///C:/data/countries.txt")
    println("Input file:"+inputFile)
    val inputSplit = inputFile.map(x=>x.split(","))
    
    println("====================schema rdd========================")
    val inputColumns = inputSplit.map(x=>filedml1(x(0), x(1), x(2), x(3)))
    
    val dataframe_schema = inputColumns.toDF()
    
    dataframe_schema.printSchema()
    val sel_col = dataframe_schema.select("state", "capital", "language")
    val fil_data = sel_col.filter("language=='English'")
    
    fil_data.show(false)
    sel_col.show()
    dataframe_schema.show()
    
    //create temp table and query using sql
    println("************* Querying using the tempView*******************")
    dataframe_schema.createOrReplaceTempView("country_table")
    spark.sql("select state, capital, language, country from country_table where language='English'").show()
    
//    fildata.coalesce(1).saveAsTextFile("file:///C:/data/English_3")
    
    //Create a dataframe using row rdd
    println("*******************Creating Dataframe using Row RDD*************************")
    val rowrdd = inputSplit.map(x=>Row(x(0), x(1), x(2), x(3)))
    
    val struct_schema=StructType(List(
        StructField("state", StringType, true),
        StructField("capital", StringType, true),
        StructField("language", StringType, true),
        StructField("country", StringType, true)
        )
    )
    
    val struct_df = spark.createDataFrame(rowrdd, struct_schema)
    
    struct_df.show(false)
    struct_df.persist()
    
    println("*****************Creating Struct View **********************")
    struct_df.createOrReplaceTempView("StructDF")
    spark.sql("select * from StructDF where country = 'India'").show()
  }
}