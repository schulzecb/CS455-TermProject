package yelp

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col

/**
 * To run:
 * $SPARK_HOME/bin/spark-submit --class yelp.GroupUserReviews ./target/original-yelp-reviews-1.0.jar 
 * \<hdfs_input_path> (yelp_user.csv)
 * \<hdfs_input_path> (top users csv)
 * \<hdfs_output_path>
 */
object GroupUserReviews {
    def main (args: Array[String]): Unit = {
        // Create new spark context
        val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Spark Count"))
        //need to create a SQL context
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        
        
        //read the yelp_user.csv file
        val review_csv = sqlContext.read.option("header", "true").csv(args(0))
        //filter the reviews to only "positive" reviews (3 stars or more)
        val positive_reviews = review_csv.where(col("stars") > 3)
        
        //find 10 users who have written 100 reviews in the yelp dataset and get their ids
        val user_10 = positive_reviews.groupBy("user_id").count().where(col("count") === 100).limit(10)
        //turn the 10 user ids into a list
        val user_list = user_10.rdd.map(r => r(0)).collect()
        
        
        //filter further based on the topusers_list
        val top_user_reviews = positive_reviews.where(col("user_id").isin(user_list:_*))
        
        //take only user_id and review from top_user_reviews
        val idReviewPair = top_user_reviews.select("user_id", "text").rdd.map((row) => (row(0), row(1)))
        //reduce by key in order to group the data
        val groupedData = idReviewPair.reduceByKey(_ + " " + _)
        
        //coalesce this into one partition
        groupedData.coalesce(1)
   
        //save the file 
        groupedData.saveAsTextFile(args(1))
    }
} 
