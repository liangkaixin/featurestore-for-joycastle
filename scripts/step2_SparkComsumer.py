import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, from_json
from pyspark.sql.types import StructType, StringType

if __name__ == '__main__':
    total_start = time.time()

    print("===> 初始化 SparkSession")
    spark_start = time.time()
    spark = SparkSession.builder \
        .appName("KafkaBatchProcessing") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    print(f"Spark 初始化完成，用时 {time.time() - spark_start:.2f} 秒")

    print("===> 定义 Kafka schema 和 topic")
    Kafka_topic = "game_events"
    schema = StructType() \
        .add("EventID", StringType()) \
        .add("PlayerID", StringType()) \
        .add("EventTimestamp", StringType()) \
        .add("EventType", StringType()) \
        .add("EventDetails", StringType()) \
        .add("DeviceType", StringType()) \
        .add("Location", StringType())

    print("===> 从 Kafka 读取数据")
    kafka_start = time.time()
    df_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", Kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    print(f"Kafka 数据加载完成，用时 {time.time() - kafka_start:.2f} 秒")

    print("===> 解析 Kafka 数据")
    parse_start = time.time()
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .withColumn("data", from_json(col("json_str"), schema)) \
        .select("data.*") \
        .withColumn("dt", to_date(col("EventTimestamp")))
    print(f"Kafka 数据解析完成，用时 {time.time() - parse_start:.2f} 秒")

    event_types = ['SessionStart', 'LevelComplete', 'InAppPurchase', 'SocialInteraction', 'SessionEnd']

    for et in event_types:
        print(f"===> 正在处理事件类型: {et}")

        filter_start = time.time()

        # 会将过滤结果拉回内存，导致OOM
        # df_parsed.filter(col("EventType") == et).collect()

        df_parsed.filter(col("EventType") == et) \
            .write.mode("overwrite") \
            .option("header", True) \
            .csv(f"Spark_output/{et}")



        print(f"Spark 过滤完 {et} 用时: {time.time() - filter_start:.2f}s")