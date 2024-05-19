from kafka import KafkaConsumer
import time
# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'], 
    auto_offset_reset='earliest', 
    enable_auto_commit=False
)

# Subscribe to a specific topic
consumer.subscribe(topics=['twitter_training'])

# Poll for new messages
while True:
    msg = consumer.poll(timeout_ms=1000)
    if msg:
        for item in msg.items():
            print(item)
            time.sleep(5)
    else:
        print("No new messages")