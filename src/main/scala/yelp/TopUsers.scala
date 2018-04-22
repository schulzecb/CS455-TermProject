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
object TopUsers {
    def main (args: Array[String]): Unit = {
        // Create new spark context
        val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Spark Count"))
        //need to create a SQL context
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        //read the yelp_user.csv file
        val user_csv = sqlContext.read.option("header", "true").csv(args(0))
        //turn this into a view - this allows us to use this as a "table"
        user_csv.createOrReplaceTempView("users")
        //generate the top 10 users
        val top = sqlContext.sql("SELECT user_id, name, review_count FROM users ORDER BY review_count DESC LIMIT 10")
        //save the file 
        top.write.format("csv").option("header", "true").save(args(1))
    }
}
 
