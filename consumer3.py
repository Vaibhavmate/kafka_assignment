
import argparse

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import csv

API_KEY = '3XBRYNB2WU373OGQ'
ENDPOINT_SCHEMA_URL = 'https://psrc-mw731.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'lfxCLUay1T/QV76o1f0xym4Yb/ypZnBtCENyfR96LbugYU7JRj3DBQiaEr1pZmuT'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'GDFNKK3HAQBGFUVJ'
SCHEMA_REGISTRY_API_SECRET = 'LNfQ102P/GWnKRLUjUzvdhtzvpmPc7cgrL5KpVnI1m4RMtLWK16tDPBTF0y3hDGI'


def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                 #  'security.protocol': 'SASL_PLAINTEXT'}
                 'bootstrap.servers': BOOTSTRAP_SERVER,
                 'security.protocol': SECURITY_PROTOCOL,
                 'sasl.username': API_KEY,
                 'sasl.password': API_SECRET_KEY
                 }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,

            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

            }


class Order:
    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

        self.record = record

    @staticmethod
    def order_to_car(data: dict, ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    topic = 'restaurent-take-away-data'
    schema_str = schema_registry_client.get_latest_version(topic + '-value').schema.schema_str
    json_deserializer = JSONDeserializer(schema_str, from_dict=Order.order_to_car)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': 'group1',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    consumer_message_count = 0
    with open('./output.csv', 'a', newline='') as f:
        w = csv.writer(f)
        w.writerow(['order_number', 'order_date', 'item_name', 'quantity', 'product_price', 'total_products'])
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                order = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

                if order is not None:
                    print("User record {}: car: {}\n"
                          .format(msg.key(), order))
                    val="User record {}: car: {}\n".format(msg.key(),order)
                    consumer_message_count += 1
                    w.writerow(val)

            except KeyboardInterrupt:
                break
        print("Total message read from resaurant_orders.csv is :", consumer_message_count)
        consumer.close()
        f.close()


main("restaurent-take-away-data")
