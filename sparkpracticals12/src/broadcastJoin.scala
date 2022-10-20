import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object broadcastJoin  extends App {

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
  .option("path","D:/week12/orders.csv")
  .load

  val customersDf = spark.read
  .format("csv")
  .option("header","true")
  .option("inferSchema","true")
  .option("path","D:/week12/customers.csv")
  .load
  
spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

val joinCondition=ordersDf.col("order_customer_id") === customersDf.col("customer_id")
val joinType="inner"

ordersDf.join(broadcast(customersDf), joinCondition,joinType).show

scala.io.StdIn.readLine()
    spark.stop()
}