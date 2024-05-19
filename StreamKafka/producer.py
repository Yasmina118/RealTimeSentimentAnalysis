import csv
from kafka import KafkaProducer
import time
# Kafka connection details
bootstrap_servers = 'localhost:9092'
topic_name = 'twitter_training'

# Open CSV file
with open('twitter_training.csv', 'r', encoding='utf-8') as csvfile:  # Adjust encoding if needed
    reader = csv.reader(csvfile)

    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    # Send each row as a message to Kafka
    for row in reader:
        message = ','.join(row).encode()  # Join CSV values with commas
        producer.send(topic_name, message)
        print(f"Sent message: {message.decode()}")
        time.sleep(5)
    # Flush and close the producer
    producer.flush()
    producer.close()

print("Finished sending data to Kafka!")
