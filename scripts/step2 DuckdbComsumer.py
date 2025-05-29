import json
import sqlite3
import time

import duckdb
from kafka import KafkaConsumer

# Kafka 配置
KAFKA_TOPIC = 'game_events'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'


# 事件字段（与 schema 对应）
event_fields = [
    "EventID", "PlayerID", "EventTimestamp", "EventType",
    "EventDetails", "DeviceType", "Location"
]

consumer = KafkaConsumer(KAFKA_TOPIC,
                         group_id = 't2',
                         auto_offset_reset='earliest',
                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                         )

# DuckDB 内存表初始化
con = duckdb.connect(database=':memory:')
con.execute(f'''
    CREATE TABLE events (
        EventID TEXT,
        PlayerID TEXT,
        EventTimestamp TEXT,
        EventType TEXT,
        EventDetails TEXT,
        DeviceType TEXT,
        Location TEXT
    )
''')

print("===> 开始消费 Kafka 消息并写入 DuckDB")
message_count = 0
start_time = time.time()
while True:
    record = consumer.poll(timeout_ms=3000)

    # 消费完退出
    if not consumer.poll(timeout_ms=3000, max_records=500):
        break
    for tp,msgs in record.items():
        for msg in msgs:

            data = msg.value
            if not all(k in data for k in event_fields):
                continue  # 跳过不完整消息

            # 插入到 DuckDB
            con.execute('''
                INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                data["EventID"], data["PlayerID"], data["EventTimestamp"],
                data["EventType"], data["EventDetails"], data["DeviceType"],
                data["Location"]
            ))
            message_count += 1

            if message_count % 10000 == 0:
                print(f"已消费 {message_count} 条，写入 SQLite 中...")
                rows = con.execute("SELECT * FROM events").fetchall()

                with sqlite3.connect("game_events.db") as conn:
                    try:
                        conn.executemany(
                            '''INSERT INTO events (
                                EventID, PlayerID, EventTimestamp, EventType,
                                EventDetails, DeviceType, Location
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)''',
                            rows
                        )
                    except sqlite3.OperationalError:
                        conn.execute(
                            '''    CREATE TABLE events (
                                    EventID TEXT,
                                    PlayerID TEXT,
                                    EventTimestamp TEXT,
                                    EventType TEXT,
                                    EventDetails TEXT,
                                    DeviceType TEXT,
                                    Location TEXT) '''
                        )