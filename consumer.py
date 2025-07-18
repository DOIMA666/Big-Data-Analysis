from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster
import json

# Cấu hình Kafka Consumer
KAFKA_BOOTSTRAP_SERVERS = "192.168.206.110:9092"
KAFKA_TOPIC = "youtube-comments-stream"
GROUP_ID = "sentiment-consumer-group"

# Cấu hình Cassandra
CASSANDRA_HOST = "localhost"
CASSANDRA_KEYSPACE = "youtube_sentiment"
CASSANDRA_TABLE = "comments"

def main():
    # Kết nối Cassandra
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)

    # Cấu hình Kafka Consumer
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    # Lưu dữ liệu vào Cassandra
    insert_query = f"INSERT INTO {CASSANDRA_TABLE} (comment, sentiment) VALUES (?, ?)"
    prepared_stmt = session.prepare(insert_query)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            # Parse message
            data = json.loads(msg.value().decode("utf-8"))
            comment = data.get("comment", "").strip()
            sentiment = data.get("sentiment", "unknown")

            # Bỏ qua những comment rỗng
            if not comment:
                print("Warning: Empty comment skipped")
                continue

            # Lưu vào Cassandra
            session.execute(prepared_stmt, (comment, sentiment))
            print(f"Saved to Cassandra: {comment} -> {sentiment}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        cluster.shutdown()

if __name__ == "__main__":
    main()
