import json

import pandas as pd
from kafka import KafkaProducer, KafkaConsumer

from constant import Kafka_topic

if __name__ == '__main__':
    df_events = pd.read_csv('game_events.csv')
    # 初始化 Kafka 生产者（需编码为字节）
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


    # convert timestamp to str for serialize
    for col in df_events.select_dtypes(include=['datetime64']).columns:
        df_events[col] = df_events[col].astype(str)

    # 逐行发送到 Kafka topic
    for _, row in df_events.iterrows():
        producer.send(Kafka_topic, value=row.to_dict())

    #  立即发送
    producer.flush()

    consumer = KafkaConsumer(Kafka_topic,
                             group_id = 'test',
                             bootstrap_servers=['localhost:9092']
                             )

    # 主动拉取消息
    records = consumer.poll(timeout_ms=10000)

    for tp, msgs in records.items():
        for msg in msgs:
            print(json.loads(msg.value))

