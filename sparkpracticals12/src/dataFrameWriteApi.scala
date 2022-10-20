import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

object dataFrameWriteApi  extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)

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
  	.option("path","D:/week11/orders.csv")
	  .load

	print("orderdf has"+ ordersDf.rdd.getNumPartitions)
	//val ordersRep = ordersDf.repartition(4)
	//print("orderdf has"+ ordersRep.rdd.getNumPartitions)
	
	ordersDf.write
	.format("avro")
	.partitionBy("order_status")
	.mode(SaveMode.Overwrite)
	.option("maxRecordsPerFile", 2000)
	.option("path","D:/week11/newFolder")
	.save()
	
   scala.io.StdIn.readLine()
    spark.stop() 
}


