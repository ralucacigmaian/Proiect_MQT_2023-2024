from confluent_kafka.avro import AvroConsumer
from confluent_kafka.serialization import SerializationError
from confluent_kafka import KafkaError

avroConsumer = AvroConsumer({'bootstrap.servers': 'localhost:9092',
                             'group.id': 'my-group',
                             'schema.registry.url': 'http://localhost:8081',
                             "api.version.request": True})
avroConsumer.subscribe(['topicProiect'])

running=True
while running:
    msg = None
    try:
        msg = avroConsumer.poll(10)
        if msg:
            if not msg.error():
                print(msg.value())
                avroConsumer.commit(msg)
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running=False
        else:
            print("No messages")
    except SerializationError as e:
        print("Serialization error for %s: %s" % (msg,e))
        running=False

avroConsumer.commit()
avroConsumer.close()