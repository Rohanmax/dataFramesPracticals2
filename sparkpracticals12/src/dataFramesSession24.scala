import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


  object dataFramesSession24 extends App{

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

    val df3 = spark.read
  .format("txt")
  .option("header", true)
  .csv("D:/week12/biglog.txt")

  df3.createOrReplaceTempView("my_new_logging_table")
  
  val columns = List("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")
  
   val result1 = spark.sql("""select level, date_format(datetime, 'MMMM')as month  
   from my_new_logging_table""").groupBy("level").pivot("month",columns).count().show(100)

  
scala.io.StdIn.readLine()
    spark.stop()
}


