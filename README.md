# Real-Time Retail Orders Pipeline using Kafka, PySpark, and Delta Lake

This is a basic real-time data engineering project that demonstrates how to build a streaming pipeline using **Confluent Kafka**, **PySpark on Databricks**, and **Delta Lake** for ingesting, transforming, storing, and publishing retail order data.


## 🚀 Project Overview

The end-to-end workflow involves:

- Ingesting retail order data from a simulated API using a **Python Kafka Producer**
- Consuming the Kafka stream in **Databricks (PySpark)**
- Parsing and flattening the data structure
- Persisting the processed data into **Delta Lake tables stored on Azure ADLS Gen2**
- Re-publishing filtered or aggregated insights back to Kafka using a **PySpark Kafka Producer**


## 🔧 Tech Stack

- **Apache Kafka (Confluent Cloud)** – Data ingestion & messaging
- **Python** – Kafka Producer
- **PySpark on Databricks** – Data processing (batch & streaming)
- **Delta Lake** – Scalable storage with ACID transactions & time travel
- **Azure ADLS Gen2** – Data lake storage

## 🧪 Sample Data

The dataset includes customer orders with the following schema
Each record represents a retail order in JSON format with nested line items:


{
  "order_id": 1,
  "customer_id": 11599,
  "customer_fname": "Mary",
  "customer_lname": "Malone",
  "city": "Hickory",
  "state": "NC",
  "pincode": 28601,
  "line_items": [
    {
      "order_item_id": 1,
      "order_item_product_id": 957,
      "order_item_quantity": 1,
      "order_item_product_price": 299.98,
      "order_item_subtotal": 299.98
    }
  ]
}

##🔄 Pipeline Workflow

Python Producer:

Reads retail order records and sends them to Kafka (retail-data-new) with key-based partitioning (by customer_id).

PySpark Consumer (Databricks):

Consumes data from Kafka topics via batch and streaming modes.

Parses JSON payloads, flattens nested structures.

Writes cleaned and structured data to Delta Lake tables in Azure ADLS Gen2.

PySpark Producer:

Filters or aggregates processed data (e.g., only orders from city = 'Chicago').

Re-publishes selected data to a different Kafka topic (processed_orders) using PySpark as a Kafka Producer.

Connect Power BI to the Delta tables on ADLS Gen2 for visualization

## 📁 Project Structure

real-time-retail-analytics/ ├── notebooks/ │ ├── kafka-consumer-batch.py # Batch consumer from Kafka to Databricks │ ├── kafka-consumer-stream.py # Streaming consumer (raw ingestion) │ ├── kafka-consumer-stream-cleaned.py # Stream ingestion with parsing and flattening │ └── kafka-producer-stream.py # PySpark-based producer (publishes processed data back to Kafka) │ ├── data/ # Sample retail order JSON files │ ├── file1.json ... file6.json │ ├── README.md └── .gitignore


This project brings together real-time data ingestion, scalable cloud processing, structured storage, and interactive analytics. It reflects a production-ready pattern used in modern data platforms, and serves as a strong foundation for future enhancements like machine learning, alerting systems, or business rule engines.
