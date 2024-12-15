from kafka.consumer import KafkaConsumer
import pandas as pd
from pandas import json_normalize
import json

# Kafka consumer configuration
topic = "newsapi-sink"
brokers = "localhost:9092"
json_file_path = "/home/aakashguru6898/final_assignment/"
file_ctr = 0

# Create the Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

# Continuously poll for new messages
for message in consumer:
    json_file_name = "newsapi_data"
    file_ctr += 1
    json_file_name += str(file_ctr)
    print("consumingggggggggggggggggggggggggggg")
    try:
        decoded_message = message.value.decode()
        if(decoded_message == 'fetch_done'):
            print('fetch_done received')
            print("---------------------------------------------------------------")
        else:
            json_object = json.loads(decoded_message)
            df = pd.DataFrame(json_object)
            normalized_df = json_normalize(df['articles'])
            print(normalized_df)
            print("---------------------------------------------------------------")
            normalized_df.to_json(json_file_path + json_file_name + '.json', orient="records", lines=True, index=False)
    except:
        print(message)
