package sparkPackage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object joinsInSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("bank_trans")
    .setMaster("local[*]")
    .set("spark.driver.host", "localhost")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val emp_df = spark.read.format("csv").option("header", "true")
    .load("file:///C:/data/joins/employees.csv")
    val dept_df = spark.read.format("csv").option("header", "true")
    .load("file:///C:/data/joins/department.csv")
    
    emp_df.printSchema
    dept_df.printSchema
    
    println("***********************Inner Join***********************")
    val inner_join = emp_df.join(dept_df,emp_df("DEPARTMENT_ID")===dept_df("Dno"),"inner")
    inner_join.show()
    println("**************************Execution Plan of Inner Join***********************")
    inner_join.explain()
    
    println("*********************Left Join***************************")
    val left_join = emp_df.join(dept_df, emp_df("DEPARTMENT_ID")===dept_df("Dno"), "left")
    left_join.show()
    
    println("*********************Right Join***************************")
    val right_join = emp_df.join(dept_df, emp_df("DEPARTMENT_ID")===dept_df("Dno"), "right")
    right_join.show(100)
    
    println("*********************Outer Join***************************")
    val outer_join = emp_df.join(dept_df, emp_df("DEPARTMENT_ID")===dept_df("Dno"), "outer")
    outer_join.show()
    
    println("********************Left Anti Join*************************")
    val left_anti_join = emp_df.join(dept_df, emp_df("DEPARTMENT_ID")===dept_df("Dno"), "left_anti")
    left_anti_join.show(100)
    
    println("********************Left Semi Join*************************")
    val left_semi_join = emp_df.join(dept_df, emp_df("DEPARTMENT_ID")===dept_df("Dno"), "left_semi")
    left_semi_join.show(100)
    
    /**
     * There are several join strategies in spark join
     * Broadcast hash join
     * Shuffle hash join: This works by shuffling both dataframes.
     * Sort merge joins
    **/
    
    
     
    
  }
}