import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object referColumnObject  extends App {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name","my first application")
    sparkConf.set("spark.master","local[2]")


    val spark = SparkSession. builder()
    .config(sparkConf)			
    .getOrCreate()			

    val ordersDf = spark.read
	.format("csv")
  	.option("header",true)
  	.option("inferSchema", true)
  	.option("path","D:/week12/orders.csv")
	.load()


	import spark.implicits._

	ordersDf.select(column("order_id"),col("order_date"),$"order_customer_id", 'order_status).show
	
	ordersDf.select(column("order_id"),expr("concat(order_status,'STATUS')")).show(false)
	
   scala.io.StdIn.readLine()
    spark.stop()
  
}