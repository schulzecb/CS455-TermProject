package yelp

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/**
 * To run:
 * $SPARK_HOME/bin/spark-submit --class yelp.TopUsers ./target/original-yelp-reviews-1.0.jar 
 * \<hdfs_input_path> (yelp_user.csv)
 * \<hdfs_output_path>
 */
object GroupUserReviews {
    def main (args: Array[String]): Unit = {
        // Create new spark context
        val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Spark Count"))
        //need to create a SQL context
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        
        //pull in the csv that contains the top users
        val topusers_csv = sqlContext.read.option("header", "true").csv(args(1))
        //transform the topusers_csv into a list that can be easily read
        val topusers_list = topusers_csv.rdd.map(r => r(0)).collect()
        
        //read the yelp_user.csv file
        val review_csv = sqlContext.read.option("header", "true").csv(args(0))
        //filter the reviews to only "positive" reviews (3 stars or more)
        val positive_reviews = reviews_csv.where($"stars" > 3)
        //filter further based on the topusers_list
        val top_user_reviews = positive_reviews.where($"user_id".isin(topusers_list:_*))
        
        
        
        
        
        
        //save the file 
        top.write.format("csv").option("header", "true").save(args(2))
    }
} 
