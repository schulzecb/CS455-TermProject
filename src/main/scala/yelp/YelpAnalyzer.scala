package yelp

import org.apache.spark.sql.SparkSession

/**
 * To run:
 * spark-submit --class yelp.YelpAnalyzer ./target/original-yelp-reviews-1.0.jar \ 
 * <input-file-from-hdfs> \
 * <output-file-from-hdfs>
 */

/**
 * Review schema
 */

object YelpAnalyzer {
    def main (args: Array[String]): Unit = {
        // Start a spark session
		val spark = SparkSession.builder().appName("Analyze Yelp Data").getOrCreate()
		// Reviews
		val reviews = spark.read.
              option("header", "true").
              option("delimiter", ",").
              csv("/yelp-data/yelp_review.csv")
    reviews.groupBy
              
    }
}
