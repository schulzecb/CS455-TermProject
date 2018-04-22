package yelp

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/**
 * To run:
 * spark-submit --class yelp.YelpAnalyzer ./target/original-yelp-reviews-1.0.jar \ 
 * <input-file-from-hdfs> \
 * <output-file-from-hdfs>
 */
object YelpAnalyzer {
    def main (args: Array[String]): Unit = {
        // Create new spark context
        val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Spark Count"))
        // Create an RDD from the input text file 
        val inputRDD = sc.textFile("")
    }
}
