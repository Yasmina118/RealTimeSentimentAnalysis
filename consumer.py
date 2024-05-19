# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lower, regexp_replace, trim, split
# from pyspark.ml import PipelineModel
# from pyspark.ml.feature import StopWordsRemover, CountVectorizer, StringIndexer

# # Initialize SparkSession with MongoDB support
# spark = SparkSession.builder \
#     .appName("ApplyModelStreaming") \
#     .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.predictions") \
#     .getOrCreate()

# # Read from Kafka
# df_stream = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:29092") \
#     .option("subscribe", "twitter_topic") \
#     .load()

# # Assuming value in Kafka is the tweet text
# df_tweets = df_stream.selectExpr("CAST(value AS STRING) as tweet")

# # Apply the same preprocessing steps as during training
# df_tweets = df_tweets.withColumn('tweet', lower(col('tweet')))
# df_tweets = df_tweets.withColumn('tweet', regexp_replace(col('tweet'), r'http\S+|www\S+|[^a-zA-Z\s]', ''))
# df_tweets = df_tweets.withColumn('words', split(col('tweet'), ' '))
# df_tweets = df_tweets.filter(df_tweets['tweet'].isNotNull() & (trim(col('tweet')) != ''))
# stop_words_remover = StopWordsRemover(inputCol='words', outputCol='filtered_words')
# df_tweets = stop_words_remover.transform(df_tweets)

# vectorizer = CountVectorizer(inputCol='filtered_words', outputCol='features')

# # Load the entire pipeline model, which includes preprocessing steps
# model = PipelineModel.load("/twitter/project/logistic_regression_model")

# # Apply the model on streaming data
# predictions = model.transform(df_tweets)
# def write_mongo(df, epoch_id):
#     df.write.format("mongo").mode("append").option("database", "twitter_db").option("collection", "predictions").save()
# # Write predictions to MongoDB
# query = predictions.writeStream.foreachBatch(write_mongo).start()


# query.awaitTermination()
# hereeeee
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, trim, split, udf, from_json
from pyspark.sql.types import ArrayType, FloatType, StringType, StructType, StructField
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StopWordsRemover, IndexToString

# Initialize SparkSession with MongoDB support
spark = SparkSession.builder \
    .appName("ApplyModelStreaming") \
    .config("spark.mongodb.output.uri", "mongodb://host.docker.internal:27017/twitter_db.predictions") \
    .getOrCreate()

# Define schema for Kafka value
schema = StructType([
    StructField("tweet", StringType(), True)
])

# Read from Kafka
df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "twitter_topic") \
    .load()

# Parse the JSON from the Kafka message
df_tweets = df_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.tweet")

# Apply the same preprocessing steps as during training
df_tweets = df_tweets.withColumn('tweet', lower(col('tweet')))
df_tweets = df_tweets.withColumn('tweet', regexp_replace(col('tweet'), r'http\S+|www\S+|[^a-zA-Z\s]', ''))
df_tweets = df_tweets.withColumn('words', split(col('tweet'), ' '))
df_tweets = df_tweets.filter(df_tweets['tweet'].isNotNull() & (trim(col('tweet')) != ''))

# Apply StopWordsRemover
stop_words_remover = StopWordsRemover(inputCol='words', outputCol='filtered_words')
df_tweets = stop_words_remover.transform(df_tweets)

# Load the entire pipeline model, which includes preprocessing steps
model = PipelineModel.load("/twitter/project/logistic_regression_model")

# Apply the model on streaming data
predictions = model.transform(df_tweets)

from pyspark.ml.feature import StringIndexerModel

# Load the StringIndexerModel directly
string_indexer_model = StringIndexerModel.load("/twitter/project/label_indexer")

# Now, use the loaded StringIndexerModel to map predictions back to original labels
index_to_string = IndexToString(inputCol="prediction", outputCol="predicted_class", labels=string_indexer_model.labels)
predictions = index_to_string.transform(predictions)

# Define UDF to convert Vector to Array (optional, if you need the features array)
def vector_to_array(v):
    return v.toArray().tolist()

vector_to_array_udf = udf(vector_to_array, ArrayType(FloatType()))

# Apply the UDF to the predictions DataFrame (optional)
predictions = predictions.withColumn("features_array", vector_to_array_udf(col("features")))

# Select necessary columns and rename if needed
predictions = predictions.select("tweet", "predicted_class")

# Define a function to write predictions to MongoDB
def write_mongo(df, epoch_id):
    df.write \
        .format("mongo") \
        .mode("append") \
        .option("database", "twitter_db") \
        .option("collection", "predictions") \
        .save()

# Write predictions to MongoDB
query = predictions.writeStream \
    .foreachBatch(write_mongo) \
    .start()

# Await termination of the query
query.awaitTermination()
