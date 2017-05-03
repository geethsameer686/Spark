//3. Find the total sales values across all the stores and the total number of sales.

package com.retail.analysis
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Float
import scala.math.random
object TotalSales {
	def main(args: Array[String]) = {
			System.setProperty("hadoop.home.dir", "G:\\Hadoop\\hadoop-2.5.0-cdh5.3.2")
			System.setProperty("spark.sql.warehouse.dir", "file:/G:/Spark/spark-2.1.0-bin-hadoop2.7/spark-warehouse")


		val spark = SparkSession
				.builder
				.appName("TotalSales")
				.master("local")
				.getOrCreate()


			val data = spark.read.textFile("G:\\DataFlair\\Practical_Scala\\Retail analysis\\Retail_Sample_Data_Set.txt").rdd

		  val result = data.map { line => {
  		val tokens = line.split("\\t")
		  ("TotalSales", Float.parseFloat(tokens(4)))
		}}

		.reduceByKey(_+_)
	  result.foreach { println }

    println("Total: " + result)
		//result.saveAsTextFile(args(1))

		spark.stop
	}
}
//Output

//(TotalSales,49585.37)