from confluent_kafka import Producer
import sys
import random
import time

def send_message(arguments):

    # Arguments
    broker = arguments[2]
    topic = arguments[1]
    conf = {
        'bootstrap.servers': broker,
        "client.id": "orders-producer"
    }

    producer = Producer(conf)
    prob = 0.1

    while True:
        temp_val = str(random.randint(40, 80) if random.random() < prob else random.randint(0, 40))
        value = f"""{{"sensor": "0", "value": "{temp_val}"}}"""
        print(value)
        producer.produce(topic, value=value)
        producer.flush()

        time.sleep(1)


if __name__ == "__main__":
    send_message(sys.argv)
