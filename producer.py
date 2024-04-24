import json
from kafka import KafkaProducer
from time import sleep

# Define your source for preprocessed data (e.g., a JSON file or a data source)
def read_preprocessed_data(source_path):
    with open(source_path, 'r') as f:
        for line in f:
            # Load each line of preprocessed data as a JSON object
            data = json.loads(line)
            yield data

# Kafka producer setup
def setup_kafka_producer(bootstrap_servers, topic):
    return KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Stream preprocessed data in real time
def stream_preprocessed_data(producer, data_generator, topic):
    for data in data_generator:
        producer.send(topic, value=data)
        print('Data sent to Kafka:', data)
        sleep(2)  # Adjust sleep interval as needed

# Main function
def main():
    # Define your source path for preprocessed data
    source_path = 'preprocessed_data.json'  # Update with your file path

    # Kafka configuration
    bootstrap_servers = ['localhost:9092']
    topic = 'test-topic'

    # Set up Kafka producer
    producer = setup_kafka_producer(bootstrap_servers, topic)

    # Read preprocessed data from the source
    data_generator = read_preprocessed_data(source_path)

    # Stream the data in real time
    stream_preprocessed_data(producer, data_generator, topic)

if __name__ == '__main__':
    main()
