//2. Calculate sales breakdown by store across all of the stores. Assume there is one store per city

package com.retail.analysis
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Float
import scala.math.random
object StoreSales {
	def main(args: Array[String]) = {
			System.setProperty("hadoop.home.dir", "G:\\Hadoop\\hadoop-2.5.0-cdh5.3.2")
			System.setProperty("spark.sql.warehouse.dir", "file:/G:/Spark/spark-2.1.0-bin-hadoop2.7/spark-warehouse")


		val spark = SparkSession
				.builder
				.appName("StoreSales")
				.master("local")
				.getOrCreate()


			val data = spark.read.textFile("G:\\DataFlair\\Practical_Scala\\Retail analysis\\Retail_Sample_Data_Set.txt").rdd

		  val result = data.map { line => {
  		val tokens = line.split("\\t")
		  (tokens(2), Float.parseFloat(tokens(4)))
		}}

		.reduceByKey(_+_)
	  result.foreach { println }

    println("Total: " + result.count())
		//result.saveAsTextFile(args(1))

		spark.stop
	}
}

//Output
// City    ,   TotalSales in that City
//(Stockton,247.18)
//(San Antonio,125.35)
//(Rochester,75.73)
//(Lincoln,712.76996)
//(Corpus Christi,25.38)
//(Jersey City,465.96)
//(Omaha,1811.8899)
//(Miami,331.75)
//(Portland,108.69)
//(San Jose,429.87)
//(Raleigh,314.1)
//(Austin,1787.88)