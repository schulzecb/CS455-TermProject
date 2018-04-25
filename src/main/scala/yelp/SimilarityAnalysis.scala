package yelp

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import scala.collection._

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}

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
        import spark.implicits._
        // read in the users-cleaned.csv from HDFS
        val user_reviews = spark.read.option("header", "true").csv("/processed-data/user/users-cleaned.csv")
        val user_reviews = spark.read.option("header", "true").csv(args(0))
        //read in the business-cleaned (small or large) from HDFS
        val business_reviews = spark.read.option("header", "true").option("multiline", "true").csv("/process-data/business/sampled-set/sampled-business.csv").sample(true, .1)
        val business_reviews = spark.read.option("header", "true").option("multiline", "true").csv(args(1))
        
        // /process-data/business/sampled-set

        //perform TFIDF on both users and businesses
        val usersTFIDF = performTFIDF(user_reviews.select("reviews"))
        val businessTFIDF = performTFIDF(business_reviews.select("reviews"))

        //create a mapping from user and business indices to ids for references later
        val userList = user_reviews.select("user_id").withColumn("id", monotonically_increasing_id()).map(r=>(r(1)+"", r.getString(0))).collect()
        val userLookup = userList.groupBy(_._1).mapValues(_.map(_._2))
        
        val businessList = business_reviews.select("business_id").withColumn("id", monotonically_increasing_id() + 10).map(r=>(r(1)+"", r.getString(0))).collect()
        val businessLookup = businessList.groupBy(_._1).mapValues(_.map(_._2))

        //create a row matrix from everything!
        val joinedRDD = usersTFIDF.union(businessTFIDF)
        val rowMatrix = new RowMatrix(joinedRDD)
        val transposedMatrix = transposeRowMatrix(rowMatrix)

        //attempt dimsum
        // val threshold = 0.8
        val estimates = transposedMatrix.columnSimilarities()

        // Filter for user indices and find closest match        
        val indexedEstimates = estimates.toIndexedRowMatrix()
        // Returns an RDD of Row (userId, closestBusinessId)
        // IDs go from 1 - 10, while indices go 0 - 9 for users
        val userIDBusinessIDList = indexedEstimates.rows.filter(_.index < 10).map(row => {
            val indexOfInterest = row.index
            val closestMaxIndex = argMaxRange(row.vector, 10, row.vector.size - 1)
            (indexOfInterest, closestMaxIndex)
        }).collect()
        // Get the id mappings
        val userIDBusinessID = userIDBusinessIDList.map(item => {
            val uid = userLookup(item._1 + "")(0)
            val bid = businessLookup(item._2 + "")(0)
            (uid, bid)
        })
        // IDs parallel
        val idsParallel = spark.sparkContext.parallelize(userIDBusinessID.map(tup => Row(tup._1, tup._2)))
        
        val schema = StructType(Seq(StructField(name = "user_id", dataType = StringType, nullable = false),StructField(name = "business_id", dataType = StringType, nullable = false)))
        val idsDF = spark.createDataFrame(idsParallel, schema)
        // Map IDs to names
        val businessData = spark.read.option("header", "true").option("multiline", "true").csv("/yelp-data/yelp_business.csv")
        val businessIDsToNames = businessData.join(idsDF, "business_id").select("business_id", "name").map(r=>(r.getString(0), r(1) + "")).collect()
        val bIdmap = businessIDsToNames.groupBy(_._1).mapValues(_.map(_._2))
        
        val userData = spark.read.option("header", "true").option("multiline", "true").csv("/yelp-data/yelp_user.csv")
        val userIDToNames = userData.join(idsDF, "user_id").select("user_id", "name").map(r=>(r.getString(0), r(1)+"")).collect()
        val uIdmap = userIDToNames.groupBy(_._1).mapValues(_.map(_._2))
        
        val userToBusinessPair = userIDBusinessID.map(row => {
            val username = uIdmap(row._1 + "")(0)
            val businessname = bIdmap(row._2 + "")(0)
            (username, businessname)
        })
        
        val finalParallel = spark.sparkContext.parallelize(userToBusinessPair.map(tup => Row(tup._1, tup._2)))
        
        finalParallel.saveAsTextFile("/thiswillneverwork/")
    }

    def argMaxRange(vector: Vector, i: Int, j: Int): Int = {
        var max: Double = 0.0
        var index = i
        for (k <- i to j) {
            if (vector.apply(k) > max) {
                index = k
                max = vector.apply(k)
            }
        }
        index
    }

    //method to perform tfidf -- returns tfidfIgnore
    //pass this method only the reviews
    def performTFIDF(df: DataFrame): RDD[Vector] = {
      // https://spark.apache.org/docs/2.2.0/mllib-feature-extraction.html#tf-idf
      val documentsRDD: RDD[Seq[String]] = df.rdd.map((row) => row.getString(0).split(" ").toSeq)
      val hashingTF = new HashingTF()
      val tf: RDD[Vector] = hashingTF.transform(documentsRDD)
      tf.cache()
      val idf = new IDF().fit(tf)
      val tfidf: RDD[Vector] = idf.transform(tf)
      val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
      val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)
      tfidfIgnore
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

