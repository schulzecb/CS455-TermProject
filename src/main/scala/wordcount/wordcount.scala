package wordcount

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/**
 * To run:
 * spark-submit --class wordcount.WordCount ./target/original-yelp-reviews-1.0.jar \ 
 * <input-file-from-hdfs> \
 * <output-file-from-hdfs>
 */
object WordCount {
    def main (args: Array[String]): Unit = {
        // Create new spark context
        val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Spark Count"))
        // Create an RDD from the input text file 
        val inputRDD = sc.textFile(args(0))
        // ... and split it into words
        val words = inputRDD.flatMap(_.split("\\W+"))
        // Count words
        val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
        // Write to a file
        wordCounts.coalesce(1).saveAsTextFile(args(1))
	// Uncomment to print out some word counts	
	// wordCounts.take(10).foreach(println(_))
    }
}
