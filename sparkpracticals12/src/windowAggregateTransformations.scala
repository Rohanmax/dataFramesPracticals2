import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.expressions.Window

object windowAggregateTransformations  extends App {

   Logger.getLogger("org").setLevel(Level.ERROR)
	
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name","my first application")
    sparkConf.set("spark.master","local[*]")

    val spark = SparkSession. builder()
    .config(sparkConf)			
    .getOrCreate()			

  val invoiceDf = spark.read 
  .format("csv")
  .option("header","true")
  .option("inferSchema","true")
  .option("path","D:/week12/windowdata.csv")
  .load()

  val myWindow = Window.partitionBy("country")
  .orderBy("weeknum")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val mydf =invoiceDf.withColumn("RunningTotal", sum("invoicevalue").over(myWindow))
  
  mydf.show()

  spark.stop()
  
}