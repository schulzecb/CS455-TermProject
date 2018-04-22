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
object TopUsers {
    def main (args: Array[String]): Unit = {
        // Create new spark context
        val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Spark Count"))
        //read the yelp_user.csv file
        val user_csv = sc.read.option("header", "true").csv(args(0))
        //turn this into a view - this allows us to use this as a "table"
        user_csv.createOrReplaceTempView("users")
        //generate the top 10 users
        val top = sc.sql("SELECT user_id, name, review_count FROM users ORDER BY review_count DESC LIMIT 10")
        //save the file 
        top.write.format("csv").option("header", "true").save(args(1))
    }
}
 
