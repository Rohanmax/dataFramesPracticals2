import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object ambiguousProblemInJoin  extends App {

   Logger.getLogger("org").setLevel(Level.ERROR)
   	
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name","my first application")
    sparkConf.set("spark.master","local[2]")

    val spark = SparkSession. builder()
    .config(sparkConf)			
    .getOrCreate()			

  val ordersDf = spark.read
  .format("csv")
  .option("header","true")
  .option("inferSchema","true")
  .option("path","D:/week12/orders_mutated.csv")
  .load
  
  
  val customersDf = spark.read
  .format("csv")
  .option("header","true")
  .option("inferSchema","true")
  .option("path","D:/week12/customers.csv")
  .load
  
  /*
  //method1
  val ordersNew = ordersDf.withColumnRenamed("customer_id","cust_id")

  val joinCondition = ordersNew.col("cust_id") === customersDf.col("customer_id")

  val joinType = "right"

  val joinedDf = ordersNew.join(customersDf, joinCondition,joinType).select("order_id", "customer_id", "customer_fname","cust_id")

  joinedDf.show
*/
  
  
  /*
  //method2
  val joinCondition=ordersDf.col("customer_id") === customersDf.col("customer_id")
  
  val joinType="outer"

  ordersDf.join(customersDf, joinCondition,joinType)
  .drop(ordersDf.col("customer_id")).select("order_id", "customer_id", "customer_fname").show
*/
  
  
  //dealing with nulls
  val joinCondition=ordersDf.col("customer_id") === customersDf.col("customer_id")
  val joinType="outer"

  ordersDf.join(customersDf, joinCondition,joinType)
  .drop(ordersDf.col("customer_id")).select("order_id","customer_id","customer_fname")
  .sort("order_id")
  .withColumn("order_id",expr("coalesce(order_id,-1)"))
  .show
  
  
  scala.io.StdIn.readLine()
    spark.stop()
  
}