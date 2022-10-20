import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object simpleAggregateTransformations  extends App {

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
 	invoiceDf.select(
	count("*").as("RowCount"),
	sum("Quantity").as("TotalQuantity"),
	avg("UnitPrice").as("AvgPrice"),
	countDistinct("InvoiceNo").as("CountDistinct")
	).show()

	
	//using string expression
	invoiceDf.selectExpr(
	"count(*) as RowCount",
	"sum(Quantity) as TotalQuantity",
	"avg(UnitPrice) as AvgPrice",
	"count(Distinct(InvoiceNo)) as CountDistinct"
	).show()
		
	//using spark SQL
	invoiceDf.createOrReplaceTempView("sales")
 
 spark.sql("select count(*), sum(Quantity), avg(UnitPrice), count(distinct(InvoiceNo)) from sales").show 
 
 spark.stop()
    
}