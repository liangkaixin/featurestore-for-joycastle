import sqlite3

import pandas as pd
from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('test',
                         group_id = 'test',
                         bootstrap_servers=['localhost:9092']
                         )
# 主动拉取消息
# records = consumer.poll(update_offsets=False)
for i in consumer:
    print(i)
# Load data
df = pd.read_csv('game_events.csv')

# 清洗示例：去除空值、修复时间格式
df.dropna(inplace=True)
df['EventTimestamp'] = pd.to_datetime(df['EventTimestamp'])

# 写入 SQLite
db_path = "game_events.db"

conn = sqlite3.connect(db_path)
df.to_sql("GameEvents", conn, if_exists="replace", index=False)
conn.close()


