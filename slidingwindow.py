from kafka import KafkaConsumer
import json
from collections import deque, Counter

# Third consumer setup: Dynamic Sliding Window approach for real-time trend detection
def dynamic_trend_detection_consumer(topic, window_size=100, alert_threshold=2):
    # Initialize Kafka consumer
    consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')
    
    # Sliding window to store recent transactions
    sliding_window = deque(maxlen=window_size)
    
    # Counter to track item occurrences within the sliding window
    item_counter = Counter()
    
    # Process incoming messages
    for message in consumer:
        # Deserialize the incoming message to a transaction
        transaction = json.loads(message.value.decode('utf-8'))
        
        # Add the transaction to the sliding window
        sliding_window.append(transaction)
        
        # Update the item counter with items from the new transaction
        item_counter.update(transaction)
        
        # Check if the sliding window is full
        if len(sliding_window) == window_size:
            # Calculate the average occurrence of each item within the window
            average_occurrences = {item: count / window_size for item, count in item_counter.items()}
            
            # Check for significant changes in item patterns
            for item, average in average_occurrences.items():
                if item_counter[item] >= alert_threshold * average:
                    # Generate alert for significant change in the pattern
                    print(f"Alert: Significant increase in '{item}' occurrence detected!")
                    print(f"Occurrence of '{item}' is {item_counter[item]}, which is more than {alert_threshold} times the average ({average}).")
            
            # Remove the oldest transaction from the sliding window
            oldest_transaction = sliding_window.popleft()
            
            # Update the item counter to reflect the removal of the oldest transaction
            item_counter.subtract(oldest_transaction)
        
        # Print the current status of the sliding window
        print(f"Current Sliding Window Size: {len(sliding_window)}")
        print(f"Item occurrences in the current window: {dict(item_counter)}")
