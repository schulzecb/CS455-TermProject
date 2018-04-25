package yelp

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * To run:
 * $SPARK_HOME/bin/spark-submit --class yelp.GroupUserReviews ./target/original-yelp-reviews-1.0.jar
 * \<hdfs_input_path> (yelp_user.csv)
 * \<hdfs_output_path>
 */
object GroupUserReviews {
    def main (args: Array[String]): Unit = {
        // Create new spark session
        val spark = SparkSession.builder().appName("Group User Reviews").getOrCreate()
        //imports (can these go up top? not sure)
        import org.apache.spark.mllib.feature.{HashingTF, IDF}
        import org.apache.spark.mllib.linalg.Vector
        import org.apache.spark.rdd.RDD

        //read in the selected_user_reviews.csv from HDFS
        val user_reviews = spark.read.option("header", "true").csv("hdfs:///term-project/selected_user_reviews.csv")
        //split users and reviews into two different dataframes
        var usersDF = user_reviews.select("user_id")
        var reviewsDF = user_reviews.select("reviews")

        //start doing TFIDF: https://spark.apache.org/docs/2.2.0/mllib-feature-extraction.html#tf-idf
        val documentsRDD: RDD[Seq[String]] = reviewsDF.rdd.map((row) => row.getString(0).split(" ").toSeq)
        val hashingTF = new HashingTF()
        val tf: RDD[Vector] = hashingTF.transform(documentsRDD)
        tf.cache()
        val idf = new IDF().fit(tf)
        val tfidf: RDD[Vector] = idf.transform(tf)
        val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
        val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)

        //now that we did that, we need to build a lookup table based on user ids and the hash codes of the tfidf vectors
        val hashesDF = tfidfHashes.toDF("hashVal")
        //create a common key in userDF and hashesDF
        usersDF = usersDF.withColumn("rowId1", monotonically_increasing_id())
        val hashesIDDF = hashesDF.withColumn("rowId2", monotonically_increasing_id())
        val userHashMapping = users.as("df1").join(hashesIDDF.as("df2"), users("rowId1") === hashesIDDF("rowId2"), "inner").select("df1.user_id", "df2.hashVal")


    }
}

