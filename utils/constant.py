from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import json
KAFKA_TOPIC = 'game_events'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
consumer = KafkaConsumer(
    group_id='t1',
    auto_offset_reset='earliest',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

partition = TopicPartition(KAFKA_TOPIC, 0)