# Pinterest_Pipeline

An end-to-end data processing pipeline that ingests data from an API simulating users posting on Pinterest, cleans and processes the data into batch and stream pipelines and stores them into a data lake and PostgreSQL database respectively. 

Technologies used:

- Python -> Programming
- Apache Kafka -> Data Ingestion
- Apache Spark (Pyspark) -> Batch Data Processing
- Apache Spark Streaming (Pyspark) -> Stream Data Processing
- Apache Airflow -> Orchestrating Batch Processing Job
- AWS S3/Boto3 -> Storage of Batch Data
- PostgreSQL -> Storage of Stream Data
