import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object referAStringColumn  extends App {

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
	  .load

	ordersDf.select("order_id","order_status").show

   scala.io.StdIn.readLine()
    spark.stop()
  
}