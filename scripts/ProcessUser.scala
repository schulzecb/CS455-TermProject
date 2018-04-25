import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.annotator._
import org.apache.spark.ml.Pipeline
// Read in data
val businessReviews = spark.read.option("header", "true").option("delimiter", ",").option("multiline", "true").csv("/grouped-reviews/user/selected_user_reviews.txt")
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
val joinArr = udf((arr: Seq[String]) => arr.mkString(" "))
val dfToWrite = output.withColumn("text", joinArr(col("finished_stems"))).select("business_id", "text")
// Write out results
dfToWrite.write.format("csv").option("header", "true").option("multiline", "true").save("/processed-data/user/")