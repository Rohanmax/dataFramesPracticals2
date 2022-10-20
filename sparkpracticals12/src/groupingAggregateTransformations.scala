import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object groupingAggregateTransformations  extends App {

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
  .option("path","D:/week12/order_data.csv")
  .load()

 //using column object expression
  val summaryDf= invoiceDf.groupBy("Country","InvoiceNo")
  .agg(sum("Quantity").as("TotalQuantity"),
	sum(expr("Quantity * UnitPrice")).as("InvoiceValue"))
	
	summaryDf.show
	
	//using string expression
	val summaryDf1= invoiceDf.groupBy("Country","InvoiceNo")
  .agg(expr("sum(Quantity) as TotalQuantity"),
	     expr("sum(Quantity * UnitPrice) as InvoiceValue")
  )
  
  summaryDf1.show
  
	//using spark SQL
  
  invoiceDf.createOrReplaceTempView("sales")

  val summaryDf2 = spark.sql("""select Country, InvoiceNo, sum(Quantity) as TotalQuantity,
  sum(Quantity * UnitPrice) as InvoiceValue from
  sales group by Country, InvoiceNo""")

  summaryDf2.show
  
spark.stop()
  
}