import sqlite3
import time
import re
from collections import defaultdict

from utils.constant import consumer, partition

BATCH_SIZE = 100

# 建表缓存
created_tables = set()


def ensure_table_exists(conn, table):
    if table in created_tables:
        return
    try:
        conn.execute(f'SELECT 1 FROM {table} LIMIT 1')
    except sqlite3.OperationalError:
        conn.execute(
            f'''CREATE TABLE IF NOT EXISTS {table} (
                EventID TEXT,
                PlayerID TEXT,
                EventTimestamp TEXT,
                EventType TEXT,
                EventDetails TEXT,
                DeviceType TEXT,
                Location TEXT
            )'''
        )
    created_tables.add(table)


def insert_batch(rows, event_type_):
    if not rows:
        return
    table = f'dwd_{event_type_}'
    with sqlite3.connect("../game_events.db") as conn:
        ensure_table_exists(conn, table)
        conn.executemany(
            f'''INSERT INTO {table} (
                EventID, PlayerID, EventTimestamp, EventType,
                EventDetails, DeviceType, Location
            ) VALUES (?, ?, ?, ?, ?, ?, ?)''', rows
        )


def main():
    event_fields = [
        "EventID", "PlayerID", "EventTimestamp", "EventType",
        "EventDetails", "DeviceType", "Location"
    ]

    consumer.assign([partition])
    consumer.seek(partition, 0)

    message_count = 0
    buffers = defaultdict(list)

    start_time = time.time()

    while True:
        record = consumer.poll(timeout_ms=3000, max_records=10000)
        if not record:
            break

        for tp, msgs in record.items():
            for msg in msgs:
                data = msg.value
                event_type = data.get('EventType')

                # 提取金额 & 停留时长 & 交互
                # Duration: 6.9 mins -> 6.9
                if event_type in ('InAppPurchase', 'SessionEnd'):
                    match = re.search(r'(\d+\.\d+|\d+)', data['EventDetails'])
                    if match:
                        data['EventDetails'] = float(match.group(0))

                if not all(k in data for k in event_fields):
                    continue

                row = [data[k] for k in event_fields]
                buffers[event_type].append(row)
                message_count += 1

                if len(buffers[event_type]) >= BATCH_SIZE:
                    insert_batch(buffers[event_type], event_type)
                    buffers[event_type].clear()

    # 剩余未写入的数据
    for event_type, rows in buffers.items():
        insert_batch(rows, event_type)

    print(f"===> Kafka -> sqlite 批量写入完成，用时: {time.time() - start_time:.2f} 秒，消息总数: {message_count}")


if __name__ == '__main__':
    # 8.56s
    main()
