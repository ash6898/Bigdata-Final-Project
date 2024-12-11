from kafka.consumer import KafkaConsumer
import pandas as pd
from pandas import json_normalize
import json

# Kafka consumer configuration
topic = "BigData-console-topic"
brokers = "localhost:9092"
csv_file_path = "/home/aakashguru6898/midterm_project/newsapi_data.csv"

# Create the Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

# Continuously poll for new messages
for message in consumer:
    decoded_message = message.value.decode()
    if(decoded_message == 'fetch_done'):
        print('fetch_done received')
        consumer.close()
    else:
        json_object = json.loads(decoded_message)
        df = pd.DataFrame(json_object)
        normalized_df = json_normalize(df['articles'])
        normalized_df.to_csv(csv_file_path, index=False)
        
    