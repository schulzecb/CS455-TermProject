package yelp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * To run:
 * spark-submit --class yelp.GroupBusinessReviews ./target/original-yelp-reviews-1.0.jar \ 
 * <input-file-from-hdfs> \
 * <output-file-from-hdfs>
 */
object GroupBusinessReviews {
	def main (args: Array[String]): Unit = {
			// Start a spark session
			val spark = SparkSession.builder().appName("Group Business Reviews").getOrCreate()
			import spark.implicits._
			// Read in the business data
			val businessDF = spark.read.option("header", "true").option("delimiter", ",").csv(args(0))
			// Filter for open restaurants
			val filteredBiz = businessDF.where(col("is_open") === "1").filter(col("categories").contains("Restaurant"))
			// Collect open restaurant ids as a list
			val restaurantIDs = filteredBiz.select("business_id").map(_.getString(0)).collect.toList
			// Read in reviews as a dataset
			val reviewsDF = spark.read.option("header", "true").option("delimiter", ",").option("multiLine", "true").csv("/yelp-data/yelp_review.csv")
			// Filter for open restaurants
			val openAndRestaurant = reviewsDF.filter(col("business_id").isin(restaurantIDs.toSeq: _*))
			// Only consider businesses with at least 100 reviews
			val largestContributed = openAndRestaurant.groupBy("business_id").count().where(col("count") >= 1000).map(_.getString(0)).collect.toList
			val filteredReviews = openAndRestaurant.where(col("business_id").isin(largestContributed.toSeq: _*))
			// Random sample 10% of the total reviews for 100 each?
			val sample = filteredReviews.sample(true, .1);
			// Select just business_id and text and convert these to a pair rdd
			val idReviewPair = sample.select("business_id", "text").rdd.map((row) => (row.getString(0), row.getString(1)))
			// Group reviews by key, and convert to a row rdd
			val groupedData = idReviewPair.reduceByKey(_ + " " + _).map(_.productIterator.toList).map(list => Row(list(0), list(1)))
			// Convert back to df
			val schema = StructType(Seq(StructField(name = "business_id", dataType = StringType, nullable = false),StructField(name = "reviews", dataType = StringType, nullable = false)))
			val dfWithSchema = spark.createDataFrame(groupedData, schema)
			// Write out to file
			dfWithSchema.write.format("csv").option("header", "true").option("multiline", "true").save(args(1))
	}
}
