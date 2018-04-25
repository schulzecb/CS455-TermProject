package yelp

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors

/**
 * To run:
 * $SPARK_HOME/bin/spark-submit --class yelp.GroupUserReviews ./target/original-yelp-reviews-1.0.jar
 * \<hdfs_input_path> (selected user reviews)
 * \<hdfs_output_path>
 */
object GroupUserReviews {
    def main (args: Array[String]): Unit = {
        // Create new spark session
        val spark = SparkSession.builder().appName("Group User Reviews").getOrCreate()

        //read in the selected_user_reviews.csv from HDFS
        val user_reviews = spark.read.option("header", "true").csv(args(0))
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
        val createdHashes = tfidfIgnore.map(vec => vec.hashCode())
        val hashesDF = createdHashes.toDF("hashVal")
        //create a common key in userDF and hashesDF
        usersDF = usersDF.withColumn("rowId1", monotonically_increasing_id())
        val hashesIDDF = hashesDF.withColumn("rowId2", monotonically_increasing_id())
        val userHashMapping = usersDF.as("df1").join(hashesIDDF.as("df2"), usersDF("rowId1") === hashesIDDF("rowId2"), "inner").select("df1.user_id", "df2.hashVal")


    }

    //METHODS TO TRANPOSE ROW MATRICES SHAMELESSLY STOLEN FROM STACK OVERFLOW <3
    //https://stackoverflow.com/questions/30556478/matrix-transpose-on-rowmatrix-in-spark
    def transposeRowMatrix(m: RowMatrix): RowMatrix = {
      val transposedRowsRDD = m.rows.zipWithIndex.map{case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex)}
      .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
      .groupByKey
      .sortByKey().map(_._2) // sort rows and remove row indexes
      .map(buildRow) // restore order of elements in each row and remove column indexes
      new RowMatrix(transposedRowsRDD)
  }


  def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
    val indexedRow = row.toArray.zipWithIndex
    indexedRow.map{case (value, colIndex) => (colIndex.toLong, (rowIndex, value))}
  }

  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
    val resArr = new Array[Double](rowWithIndexes.size)
    rowWithIndexes.foreach{case (index, value) =>
        resArr(index.toInt) = value
    }
    Vectors.dense(resArr)
  }
}

