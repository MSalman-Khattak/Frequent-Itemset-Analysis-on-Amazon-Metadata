from kafka import KafkaConsumer
from pymongo import MongoClient
import json


def integrate_results_to_mongodb(consumer, topic, algorithm_name, db_name='results_db'):
    mongo_uri = "mongodb://localhost:27017"
    client = MongoClient(mongo_uri)

    db = client[IntegratedResults]

    collection = db[f"{algorithm_name}_results"]
    
    transactions = []
    
    for message in consumer:

        data = json.loads(message.value.decode('utf-8'))
            

        results = process_data(data)
        

        if results:
            collection.insert_many(results)
        
        print(f"Results for {algorithm_name}:", results)


apriori_consumer = KafkaConsumer("apriori_topic", bootstrap_servers='localhost:9092')
integrate_results_to_mongodb(apriori_consumer, "apriori_topic", "apriori")

pcy_consumer = KafkaConsumer("pcy_topic", bootstrap_servers='localhost:9092')
integrate_results_to_mongodb(pcy_consumer, "pcy_topic", "pcy")

sliding_window_consumer = KafkaConsumer("sliding_window_topic", bootstrap_servers='localhost:9092')
integrate_results_to_mongodb(sliding_window_consumer, "sliding_window_topic", "sliding_window")

