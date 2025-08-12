from kafka import KafkaProducer
import time
import csv

file_path="/home/sunbeam/Big_Data_Project_data/archive/economy.csv"
producer=KafkaProducer(bootstrap_servers='localhost:9092')
topic="flight-economy"

with open(file_path, "r") as f:
    reader=csv.reader(f)
    header=next(reader)
    for row in reader:
        line=",".join(row)
        producer.send(topic, value=line.encode('utf-8'))
        time.sleep(0.001)
producer.flush()
producer.close()