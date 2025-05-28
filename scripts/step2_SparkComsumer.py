import sqlite3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
import time

if __name__ == '__main__':
    # 1. 初始化 SparkSession（包含 Kafka 支持）
    spark = SparkSession.builder \
        .appName("KafkaBatchProcessing") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    # 2. 定义 Kafka topic 和 schema
    Kafka_topic = "game_events"

    schema = StructType() \
        .add("EventID", StringType()) \
        .add("PlayerID", StringType()) \
        .add("EventTimestamp", StringType()) \
        .add("EventType", StringType()) \
        .add("EventDetails", StringType()) \
        .add("DeviceType", StringType()) \
        .add("Location", StringType())

    # 3. 从 Kafka 拉取数据
    df_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", Kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # 4. 解析 JSON 字符串（value 是 binary 需要转成 string）
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .withColumn("data", from_json(col("json_str"), schema)) \
        .select("data.*").withColumn("dt", to_date(col("EventTimestamp")))



    from pyspark.sql.functions import col

    event_types = ['SessionStart', 'LevelComplete', 'InAppPurchase', 'SocialInteraction', 'SessionEnd']

    # spark -> sqlite 有性能问题
    for et in event_types:
        table_name = f'dwd_events_{et}'
        df_filtered = df_parsed.filter(col("EventType") == et).toPandas()


        start = time.time()
        with sqlite3.connect('game_events.db') as conn:
            # SQLite-specific optimizations
            conn.execute("PRAGMA journal_mode = WAL")  # Better concurrency
            conn.execute("PRAGMA synchronous = NORMAL")  # Good reliability/speed balance

            df_filtered.to_sql(
                name=table_name,
                con=conn,
                if_exists='replace',
                index=False,
                chunksize=1000,  # Important for SQLite
                method='multi'   # Batch inserts
            )
            print(f"写入事件类型 {et} 到 SQLite 表 {table_name} 耗时 {time.time() - start} 秒")
    conn.close()
