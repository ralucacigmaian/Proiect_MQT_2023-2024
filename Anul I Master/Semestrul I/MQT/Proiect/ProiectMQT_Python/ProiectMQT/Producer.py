from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import json

with open('Info.json', 'r') as file:
    json_data = json.load(file)

value_schema = avro.load('avro/EvenimentAvro.avsc')

avroProducer = AvroProducer({'bootstrap.servers': 'localhost:9092', 'schema.registry.url': 'http://localhost:8081'},
                            default_value_schema=value_schema)

for record in json_data:
    avroProducer.produce(topic='topicProiect', value=record, value_schema=value_schema)

avroProducer.flush()