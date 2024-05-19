import json
import pandas as pd
from kafka import KafkaProducer
import time
# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Path to the CSV file
csv_file_path = 'twitter_validation.csv'

# Define custom column names
column_names = ["ID", "name", "class", "tweet"]

try:
    # Read the CSV file using pandas without headers
    df = pd.read_csv(csv_file_path, header=None, names=column_names)

    # Iterate over each row in the DataFrame
    for _, row in df.iterrows():
        # Create a message dictionary
        message = {
            "tweet": row["tweet"]
        }

        # Send message to Kafka
        try:
            producer.send('twitter_topic', value=message)
            time.sleep(5)
            print(f"Sent: {message}")
        except Exception as e:
            print(f"Error sending message: {e}")

    # Ensure all messages are sent
    producer.flush()
except Exception as e:
    print(f"Error reading CSV file: {e}")
finally:
    # Close the producer
    producer.close()
