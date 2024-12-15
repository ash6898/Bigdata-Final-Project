from kafka.producer import KafkaProducer
import requests
import json
import time

def get_api_data(base_url, query_params, topic, producer):
    try:
        while True:
            # Make a request to the News API
            response = requests.get(base_url, params=query_params)
            response.raise_for_status()  # Raise an HTTPError for bad responses
    
            json_data = response.json()
            
            # Check if there are more pages
            if not json_data['articles']:
                print('------ fetching done ------')
                producer.send('fetch_done'.encode('utf-8'))
                producer.flush()
                break
            
                        
            json_str = json.dumps(json_data)
            
            # Convert the JSON string to bytes
            json_bytes = json_str.encode('utf-8')
    
            producer.send(topic, json_bytes)
            producer.flush()
    
            # Move to the next page
            query_params['page'] += 1
            time.sleep(1)

    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code
    
        if status_code == 426:
            print('----- api limit reached -----')
            producer.send(topic, 'fetch_done'.encode('utf-8'))
            producer.flush()
            
        else:
            print(f"HTTP Error Code: {status_code}")
    

# Kafka producer configuration
topic = "newsapi-sink"
brokers = "localhost:9092"

api_key = "eba6f01a1f994b548927a88c1dc8b45a"
base_url = "https://newsapi.org/v2/everything"
query_params = {
    "q": "football",
    "sortBy": "popularity",
    "apiKey": api_key,
    "pageSize": 50,
    "page": 1
}

# Create the Kafka producer
producer = KafkaProducer(bootstrap_servers=brokers)

get_api_data(base_url, query_params, topic, producer)