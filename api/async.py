from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

def kafka_push(host, topic, msg):
    kafka = KafkaClient(host)
    producer = SimpleProducer(kafka)
    producer.send_messages(topic, str(msg))

