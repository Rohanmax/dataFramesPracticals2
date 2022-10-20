import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object dataFramesSession23 extends App{

	case class Logging(level:String, datetime:String)

	def mapper(line:String): Logging = {
	val fields = line.split(',')
	val logging:Logging = Logging(fields(0), fields(1))
	return logging
}

   Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession. builder()
    .appName("SparkSQL")
    .master("local[*]")		
    .getOrCreate()			

import spark.implicits._
/*
val mylist = List("WARN,2016-12-31  04:19:32",
"FATAL,2016-12-31 03:22:34",
"WARN,2015-4-21 14:32:21",
"INFO,2015-4-21 14:32:21",
"FATAL,2015-4-21 19:23:20")

val rdd1 = spark.sparkContext.parallelize(mylist)

val rdd2 = rdd1.map(mapper)
val df1 = rdd2.toDF()
*/

  val df3 = spark.read
  .format("txt")
  .option("header", true)
  .csv("D:/week12/biglog.txt")

  
  df3.createOrReplaceTempView("my_new_logging_table")
  
  val results = spark.sql("select level, date_format(datetime, 'MMMM')as month, count(1) as total from my_new_logging_table group by level,month")

 val result1 = spark.sql("""select level, date_format(datetime, 'MMMM')as month, 
  cast(first(date_format(datetime, 'M'))as int) as monthnum, count(1) as total 
 from my_new_logging_table group by level,month order by monthnum""")

  val result2 = result1.drop("monthnum")
  
  result2.show()
  
scala.io.StdIn.readLine()
    spark.stop()
}


