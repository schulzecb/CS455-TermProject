package yelp

import org.apache.spark.sql.SparkSession
// Jon Snow NLP Dependencies
import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.Finisher
import com.johnsnowlabs.nlp.annotators.spell.norvig.NorvigSweetingApproach
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.Normalizer
import com.johnsnowlabs.nlp.annotators.Stemmer
// import com.johnsnowlabs.nlp.base._
// import com.johnsnowlabs.nlp.annotator._
// ML Pipeline
import org.apache.spark.ml.Pipeline
// For UDF &  COL
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row


/**
 * To run:
 * spark-submit --class yelp.PreprocessBusinessReviews ./target/original-yelp-reviews-1.0.jar \ 
 * --packages JohnSnowLabs:spark-nlp:1.5.1 or 
 * --jars ./spark-nlp-1.5.1.jar
 */
object PreprocessReviews {
	def main (args: Array[String]): Unit = {
		// Start a spark session
		val spark = SparkSession.builder().appName("Preprocess Business Reviews").getOrCreate()
		import spark.implicits._
		// Read in data
		val businessReviews = spark.read.option("header", "true").option("delimiter", ",").option("multiline", "true").csv(args(0))
		// Needed to create pipeline
		val documentAssembler = new DocumentAssembler().setInputCol("reviews")
		// Tokenize
		val tokenizer = new Tokenizer().setInputCols("document").setOutputCol("token")
		// Remove dirty characters
		val normalizer = new Normalizer().setInputCols("token").setOutputCol("normal")
		// Correct poor spelling
		var spell_checker = new NorvigSweetingApproach().setInputCols("normal").setOutputCol("spell").setDictionary("/dictionaries/word.list")
		// Stem words
		val stemmer = new Stemmer().setInputCols("spell").setOutputCol("stems")
		// Get out a clean result
		val finisher = new Finisher().setInputCols("stems")
		// We need some training text to train on
		val training = spark.sparkContext.textFile("/dictionaries/holmes.txt").map(_.replace("[^\\w\\s]", "")).toDF("reviews")
		// Create a pipeline
		val pipeline = new Pipeline().setStages(Array(documentAssembler, tokenizer, normalizer, spell_checker, stemmer, finisher))
		// Train pipeline and get output data
		val output = pipeline.fit(training).transform(businessReviews)
		// Transform arrays to regular strings
		val joinArr = (arr: Seq[String]) => arr.mkString(" ")
		val dfToWrite = output.select("business_id", "finished_stems").rdd.map({
			case Row(business_id: String, finished_stems: Seq[String]) =>
				(business_id, joinArr(finished_stems))
		}).toDF("business_id", "reviews")
		// Write out results
		dfToWrite.write.format("csv").option("header", "true").option("multiline", "true").save(args(1))
	}
}
