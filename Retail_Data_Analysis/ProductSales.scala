//1. Calculate sales breakdown by product category across all of the stores.

package com.retail.analysis
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Float
import scala.math.random
object ProductSales {
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
		  (tokens(3), Float.parseFloat(tokens(4)))
		}}

		.reduceByKey(_+_)
	  result.foreach { println }

    println("Total: " + result.count())
		//result.saveAsTextFile(args(1))

		spark.stop
	}
}
//bin/spark-submit --class package com.retail.analysis.ProductSales ../retailJob.jar ../Retail_Sample_Data_Set.txt ret-out-01


//Output
//   Product      , TotalSales
//(Men's Clothing,4030.89)
//(Video Games,2573.38)
//(Computers,2102.66)
//(Women's Clothing,3736.8699)
//(Sporting Goods,1952.89)
//(Music,2396.4)
//(CDs,2644.51)
//(Garden,1882.25)
//(Pet Supplies,2660.83)