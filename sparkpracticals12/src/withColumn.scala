import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.log4j.Level
import org.apache.log4j.Logger

case class Person(name: String, age:Int, city:String)

object withColumn  extends App {

   Logger.getLogger("org").setLevel(Level.ERROR)
   
	def ageCheck(age: Int):String ={
	if (age > 18)"Y" else "N"
}

    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name","my first application")
    sparkConf.set("spark.master","local[2]")

    val spark = SparkSession. builder()
    .config(sparkConf)			
    .getOrCreate()			

    val df = spark.read
	  .format("csv")
    .option("inferSchema", true)
  	.option("path","D:/week12/dataset.DATASET1")
  	.load()

	import spark.implicits._
	
  	val df1: Dataset[Row] = df.toDF("name","age","city")

 //COLUMN OBJECT EXPRESSION UDF
	
  	//val parseAgeFunction = udf(ageCheck(_:Int):String)
	  //val df2= df1.withColumn("adult",parseAgeFunction(col("age")))
	  //df2.show
  	
//SQL STRING EXPRESSION UDF
  	
  	//named function
  	spark.udf.register("parseAgeFunction", ageCheck(_:Int):String)
  	
  	//anonymous function
  	//spark.udf.register("parseAgeFunction", (x:Int) => {if (x > 18) "Y" else "N"})
  	
  	val df2 = df1.withColumn("adult", expr("parseAgeFunction(age)"))
  	
  	df2.show
  	
  	spark.catalog.listFunctions().filter(x => x.name == "parseAgeFunction").show
  	
  	df1.createOrReplaceTempView("peopleTable")
  	
  	spark.sql("select name, age, city,parseAgeFunction(age) as adult from peopleTable").show

    spark.stop()
  
}