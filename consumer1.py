from apyori import apriori
from kafka import KafkaConsumer
import json

def extract_transactions(data):
    # Select keys suitable for transactions
    keys_for_transactions = ['category', 'fit', 'also_buy', 'feature', 'also_view']
    
    transactions = []
    for key in keys_for_transactions:
        if key in data:
            if isinstance(data[key], list):
                transactions.extend(data[key])
            else:
                transactions.append(data[key])
    
    return transactions

def print_results(results):
    for result in results:
        print("Itemset:", result.items)
        print("Support:", result.support)
        print()

def main():
    # Kafka consumer configuration
    bootstrap_servers = ['localhost:9092']
    topic = 'test-topic'

    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    try:
        for message in consumer:
            data = message.value
            print("Received data:", data)  # Debug print

            # Extract transactions
            transactions = extract_transactions(data)
            print("Transactions:", transactions)  # Debug print
            
            # Apply Apriori algorithm with different support thresholds
            for min_support in [0.05, 0.1, 0.15]:
                results = list(apriori(transactions, min_support=min_support, min_confidence=0.5))
                print("Results with min_support={}: {}".format(min_support, results))  # Debug print

                # Print results if any frequent itemsets are found
                if results:
                    print("Frequent Itemsets (min_support={}):".format(min_support))
                    print_results(results)
                else:
                    print("No frequent itemsets found with min_support={}".format(min_support))

    finally:
        consumer.close()

if __name__ == "__main__":
    main()

