#!/bin/bash
set -euo pipefail
echo "Starting HDFS"
start-dfs.sh
echo "Starting YARN"
start-yarn.sh
echo "Starting ZooKeeper"
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 5

echo "Starting Kafka broker"
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
sleep 5

echo "Creating Kafka topics"
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic flight-business --partitions 1 --replication-factor 1 || true
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic flight-economy --partitions 1 --replication-factor 1 || true
sleep 3

echo " Running Kafka producers in separate terminals"
gnome-terminal -- bash -c "cd ~/BigData_Project && python3 kafka_business.py; exec bash"
gnome-terminal -- bash -c "cd ~/BigData_Project && python3 kafka_economy.py; exec bash"
sleep 10

echo "Running Spark Streaming for business class in separate terminal"
gnome-terminal -- bash -c "cd ~/BigData_Project && spark-submit \
  --master local[*] \
  --driver-memory 8G \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.default.parallelism=16 \
  --conf spark.streaming.backpressure.enabled=true \
  --conf spark.streaming.kafka.maxRatePerPartition=200 \
  spark_streaming_business.py; exec bash"

echo "Running Spark Streaming for economy class in separate terminal"
gnome-terminal -- bash -c "cd ~/BigData_Project && spark-submit \
  --master local[*] \
  --driver-memory 8G \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.default.parallelism=16 \
  --conf spark.streaming.backpressure.enabled=true \
  --conf spark.streaming.kafka.maxRatePerPartition=200 \
  spark_streaming_economy.py; exec bash"

echo "All scripts running"
