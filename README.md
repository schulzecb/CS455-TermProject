# CS455-TermProject

## Comparing Yelp Reviews with Jon Snow Natural Language Processing & Consine Similarity

This project is an experiment in providing **review recommendations** by comparing the corpus of a users postive reviews to the reviews of a business. The principle hypothesis is that the text the user uses to describe businesses they like can be compared to a general description other users have been given for a business.

## Analysis

The analysis for this project occurs in three parts.

1. Filter Reviews and Wrangle Text
2. Preprocess Review Data
3. Perform TIDF Vectorization and All-Pairs Similarity Analysis

### Filter Reviews and Text Wrangling

For businesses we filtered for those with the category 'restaurant' and with a corpus of at least 1000 reviews. We then randomly selected from that set of total businesses before grouping the reviews by business id. We did this to reduce to the total corpus size for processing reasons. For users we found those who had at least 100 reviews over 3 stars present in the yelp review data set. We then randomly selected 10 of these users. The final output for each of these is a csv containing: "business_id | user_id", "<all-reviews-for-that-id>"

The code for businesses can be ran with:

``` 
    spark-submit --class yelp.GroupBusinessReviews ./target/original-yelp-reviews-1.0.jar \ 
    <input-file-from-hdfs> \
    <output-file-from-hdfs> 
```

```  
   $SPARK_HOME/bin/spark-submit --class yelp.TopUsers ./target/original-yelp-reviews-1.0.jar \
   <hdfs_input_path> (yelp_user.csv) \
   <hdfs_output_path>
   ...
   $SPARK_HOME/bin/spark-submit --class yelp.GroupUserReviews ./target/original-yelp-reviews-1.0.jar \
   <hdfs_input_path> (top_users.csv) \
   <hdfs_output_path>
```

*Note that these processing steps reley on the hardcoded path '/yelp-data/yelp_review.csv' for yelp-data in hdfs*

### Preprocess Review Data

After wrangling all the reviews we preprocessed the data using Jon Snow Labs language processing library. In the preprocessing step we tokenize, normalize, spell-correct and stem the reviews for both businesses and users. This gives us 'cleaned' data that has no punctuation or foreign characters, as well using stemming to reduce variance for similiar words like 'run' and 'running'.

```
   spark-submit --class yelp.PreprocessBusinessReviews ./target/yelp-reviews-1.0.jar \ 
   --packages JohnSnowLabs:spark-nlp:1.5.1 \
   <input.csv: business_id, reviews> <outputfile.csv>
   
   spark-submit --class yelp.PreprocessUserReviews ./target/yelp-reviews-1.0.jar \ 
   --packages JohnSnowLabs:spark-nlp:1.5.1 \
   <input.csv: user_id, reviews> <outputfile.csv>
```

### Analyze Data

For the final step we use term-frequency inversion, vectorization, and the all-pairs similiarity algorithm provided by spark to compare the reviews for users and business. Because of heap space issues related to large corpus for business reviews we had to random sample from our business data set. This can be corrected in future implementations by adding stop-word removal to the preprocessing step as well as dimensionality reduction in the analysis.


```
   $SPARK_HOME/bin/spark-submit --class yelp.GroupUserReviews ./target/original-yelp-reviews-1.0.jar
    \<hdfs_input_path> (clean user reviews)
    \<hdfs_input_path> (clean business reviews)
    \<hdfs_input_path> (yelp_business)
    \<hdfs_input_path> (yelp_user)
    \<hdfs_output_path>
```
