package wordcount

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

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
        wordsCounts.saveAsTextFile(args(1))
    }
}
