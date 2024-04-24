# Frequent-Itemset-Analysis-on-Amazon-Metadata

Real-time Data Processing with Apache Kafka

This repository contains Python scripts for real-time data processing using Apache Kafka. The scripts demonstrate various scenarios of data ingestion, preprocessing, analysis, and integration with external systems.

Requirements

Python 3.x
Apache Kafka
MongoDB (optional, for data integration)
Pandas, json (for data preprocessing)
Apyori (for association rule mining)
Kafka-Python (for Kafka integration)
Installation

Install Python dependencies:

pip install pandas json apyori kafka-python pymongo
Set up Apache Kafka. Follow the official documentation for installation instructions.
(Optional) Set up MongoDB if you plan to integrate results with a database. Follow the official documentation for installation instructions.
Usage

Preprocessing Data (app1.py)

This script preprocesses JSON data and normalizes it for further analysis.

python app1.py
Association Rule Mining (consumer1.py)

This script consumes preprocessed data from Kafka, extracts transactions, and applies the Apriori algorithm to discover frequent itemsets.

python consumer1.py
Data Integration (dataintegration.py)

This script integrates results from different Kafka topics into MongoDB.

python dataintegration.py
PCY Algorithm (pcy.py)

This script implements the PCY algorithm for frequent itemset mining using a Kafka consumer.

python pcy.py
Data Producer (producer1.py)

This script reads preprocessed data and streams it to Kafka for real-time processing.

python producer1.py
Sliding Window Approach (slidingwindow.py)

This script implements a dynamic sliding window approach for real-time trend detection using Kafka.

python slidingwindow.py
Configuration

Adjust Kafka server configurations in the respective scripts (bootstrap_servers variable).
Modify topics and thresholds as per your requirements.
