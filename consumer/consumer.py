from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import os
import json

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = 'pokemon-topic'
JSON_FILE = 'datas.json'

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'pokemon',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([KAFKA_TOPIC])

products = []

def write_to_json_file(products):
    with open(JSON_FILE, 'w') as file:
        json.dump(products, file, indent=4)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        product = json.loads(msg.value().decode('utf-8'))
        products.append(product)

        write_to_json_file(products)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

print(f"Consumed messages and wrote to {JSON_FILE}")
