### Overview

Developed a data pipeline using Apache Airflow, Kafka, Spark Streaming, and Cassandra to process and store real-time data from an external API. The pipeline consists of the following components:

- Airflow DAG: Scheduled to fetch user data from the RandomUser API and continuously stream it to a Kafka topic.
- Kafka Producer: Sends the fetched and formatted user data to a Kafka topic (users_queue).
- Spark Streaming: Reads the streaming data from the Kafka topic, processes it, and loads it into a Cassandra database.
- Cassandra Database: Stores the processed user data in a structured format, facilitating efficient querying and analysis.


### Architecture


```
Key Technologies: Apache Airflow, Kafka, Spark Streaming, Cassandra, Python
```

### Requirements

- Python 3.11
- Apache Hadoop 3.3.0
- Apache Spark
- Docker



The project consists of two main scripts: `kafka-streamer.py` and `spark-streaming.py`.

- `kafka-streamer.py`: Fetches user data from a public API, formats it, and sends it to a Kafka topic named `users_queue`. This is scheduled to run continuosly and publish data to Kafka topic
- `spark-streaming.py`: Creates Cassandra keyspace and table, reads data from the Kafka topic, processes it using Spark, and inserts it into a Cassandra database.

All the architecture is containerized in Docker.

### Usage

### Streaming data into Kafka

1. Set up the environment and architecture
```
  docker-compose up -d 
  ```
2. Open Apache Airflow (http://localhost:8080/) and select Trigger DAG(load_users_data)
3. Open Control Center (http://localhost:9021/) > Topics > users_queue and watch the messages popping up

### Streaming data into Cassandra

1. Run the below comand to start Spark and Trigger DAG in Apache Airflow
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 spark-stream.py
```
2. Access the Cassandra container:
```
docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
```
3. Check that the data in users_queue topic is available in Cassandra:
```
SELECT * FROM profiles.users;
```

### Tools

- Docker Desktop
- Poetry