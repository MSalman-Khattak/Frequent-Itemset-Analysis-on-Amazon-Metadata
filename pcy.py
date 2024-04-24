from kafka import KafkaConsumer
import pandas as pd
from collections import defaultdict, Counter
import json

# Function to implement the PCY algorithm
def pcy_algorithm(transactions, min_support, num_buckets=1000):
    # Step 1: Count the occurrence of each item
    item_counts = Counter()
    for transaction in transactions:
        for item in transaction:
            item_counts[item] += 1

    # Filter items based on minimum support
    frequent_items = {item for item, count in item_counts.items() if count >= min_support}
    
    # Step 2: Count pairs and create a hash table
    pairs_counts = Counter()
    hash_table = defaultdict(int)
    
    for transaction in transactions:
        # Only consider frequent items in each transaction
        frequent_transaction = [item for item in transaction if item in frequent_items]
        
        # Count pairs and update hash table
        for i, item1 in enumerate(frequent_transaction):
            for item2 in frequent_transaction[i + 1:]:
                pair = frozenset([item1, item2])
                pairs_counts[pair] += 1
                hash_value = hash(pair) % num_buckets
                hash_table[hash_value] += 1
    
    # Step 3: Filter pairs based on minimum support
    frequent_pairs = {pair for pair, count in pairs_counts.items() if count >= min_support}
    
    # Output the results
    print("Frequent Pairs:", frequent_pairs)

# Kafka consumer setup
def pcy_consumer(topic, min_support, num_buckets=1000):
    consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')
    transactions = []
    
    for message in consumer:
        # Convert the received message to a transaction
        transaction = json.loads(message.value.decode('utf-8'))
        transactions.append(transaction)
        
        # Implement PCY algorithm on the transactions
        pcy_algorithm(transactions, min_support, num_buckets)
        
        # Print real-time insights and associations
        print("Frequent Pairs:", frequent_pairs)
