from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, trim, split
from pyspark.ml.feature import StopWordsRemover, CountVectorizer, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import os
import shutil

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysis") \
    .getOrCreate()

# Define schema for the CSV file
schema = "ID STRING, name STRING, class STRING, tweet STRING"

# Load CSV file into Spark DataFrame
df = spark.read.csv("/twitter/project/twitter_training.csv", schema=schema, header=False)

# Drop rows with null or empty 'tweet' values
df = df.filter(df['tweet'].isNotNull() & (trim(col('tweet')) != ''))

# Convert 'tweet' column to lowercase
df = df.withColumn('tweet', lower(col('tweet')))

# Remove links and special characters using regex
df = df.withColumn('tweet', regexp_replace(col('tweet'), r'http\S+|www\S+|[^a-zA-Z\s]', ''))

# Tokenize the tweets
df = df.withColumn('words', split(col('tweet'), ' '))

# Remove stopwords using StopWordsRemover
stop_words_remover = StopWordsRemover(inputCol='words', outputCol='filtered_words')
df = stop_words_remover.transform(df)

# Encode string labels to numeric labels
label_indexer = StringIndexer(inputCol="class", outputCol="label")
label_indexer_model = label_indexer.fit(df)

# Print label indexer mappings for debugging
print("Label Indexer Mappings:")
for label, index in zip(label_indexer_model.labels, range(len(label_indexer_model.labels))):
    print(f"Label: {label} -> Index: {index}")

# Transform the dataframe using the label indexer model
df = label_indexer_model.transform(df)

# Vectorize text using CountVectorizer
vectorizer = CountVectorizer(inputCol='filtered_words', outputCol='features')

# Define the Logistic Regression model
lr = LogisticRegression(featuresCol='features', labelCol='label')

# Create a pipeline with CountVectorizer and Logistic Regression
pipeline = Pipeline(stages=[vectorizer, lr])

# Train the model
model = pipeline.fit(df)

# Paths for saving the model and label indexer within the container
model_path = "/twitter/project/logistic_regression_model"
label_indexer_path = "/twitter/project/label_indexer"

# Ensure the directories do not exist before saving
if os.path.exists(model_path):
    shutil.rmtree(model_path)
if os.path.exists(label_indexer_path):
    shutil.rmtree(label_indexer_path)

# Save the model locally within the container
model.save(model_path)

# Ensure label indexer path exists and save the indexer model
os.makedirs(label_indexer_path, exist_ok=True)
label_indexer_model.write().overwrite().save(label_indexer_path)

print("Model and label indexer saved successfully.")

# Stop SparkSession
spark.stop()
