import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.types.DateType

object dataFrameSession15  extends App {

   Logger.getLogger("org").setLevel(Level.ERROR)
	
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name","my first application")
    sparkConf.set("spark.master","local[*]")

    val spark = SparkSession. builder()
    .config(sparkConf)			
    .getOrCreate()			

val myList = List((1,"2013-07-25",11599,"CLOSED"),
    (2,"2014-07-25",256,"PENDING_PAYMENT"),
    (3,"2013-07-25",11599,"COMPLETE"),
    (4,"2013-07-25",8827,"CLOSED"))

   val ordersDf = spark.createDataFrame(myList)
   .toDF("orderid","orderdate","customerid","status")

   val newDf = ordersDf
   .withColumn("orderdate",unix_timestamp(col("orderdate")
   .cast(DateType)))
   .withColumn("newid",monotonically_increasing_id)
   .dropDuplicates("orderdate","customerid")
   .drop("orderid")
  .sort("orderdate")

  newDf.printSchema()
  newDf.show()

spark.stop()
  
}