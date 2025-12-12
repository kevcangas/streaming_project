# Streaming Project

This project consists in the use of Docker to provision a Kafka environment to send and consume streaming data using Python. 

## Producer

Is in charged of create the fake data to send it to Kafka.

## Fraud Detector

Is constantly analyzing the data sent to Kafka, if there is a value in amount greater than $5,000.00, the script shows a message mentioning that the current transaction is a fraud.

## Archiver

This script is in charged of storage the data in MINio, to do this, the script saves the data in batchs with size 10 rows by each one.

## Execution

1. docker compose up

2. docker compose exec app python producer.py

3. docker compose exec app python fraud_detector.py

4. docker compose exec app python archiver.py