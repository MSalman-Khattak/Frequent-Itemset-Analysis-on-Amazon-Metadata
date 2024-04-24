from kafka import KafkaProducer
import json
import pandas as pd

def preprocess_data(input_file, producer):
    # Read the JSON data
    data = pd.read_json(input_file, lines=True)

    for _, row in data.iterrows():
        # Preprocess each row
        preprocessed_row = preprocess_row(row)
        
        # Send preprocessed data to Kafka
        producer.send('preprocessed_data_topic', json.dumps(preprocessed_row).encode('utf-8'))
        producer.flush()

def preprocess_row(row):
    # Perform preprocessing steps for each row
    # For simplicity, let's just convert everything to lowercase
    preprocessed_row = {key: value.lower() if isinstance(value, str) else value for key, value in row.items()}
    return preprocessed_row

def main():
    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Path to your JSON file
    input_file = 'your_json_file.json'

    preprocess_data(input_file, producer)

    # Close the producer
    producer.close()

if __name__ == "__main__":
    main()

