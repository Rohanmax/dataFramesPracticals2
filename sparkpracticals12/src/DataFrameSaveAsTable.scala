import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object DataFrameSaveAsTable  extends App {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name","my first application")
    sparkConf.set("spark.master","local[2]")

    val spark = SparkSession. builder()
    .config(sparkConf)	
    .enableHiveSupport()		
    .getOrCreate()			

    val ordersDf = spark.read
	  .format("csv")
  	.option("header",true)
  	.option("inferSchema", true)
  	.option("path","D:/week11/orders.csv")
	  .load()

 	 ordersDf.write
	 .format("csv")
	 .mode(SaveMode.Overwrite)
	 .saveAsTable("orders")

   scala.io.StdIn.readLine()
    spark.stop()
}