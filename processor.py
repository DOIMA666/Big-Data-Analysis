from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, to_json, struct
from pyspark.sql.types import StructType, StringType
from textblob import TextBlob

# Tạo Spark session
spark = (
    SparkSession.builder
    .appName("YouTubeSentimentAnalysis")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")  # Tự động suy ra schema nếu cần
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

# Định nghĩa schema
schema = (
    StructType()
    .add("original_comment", StringType())
    .add("cleaned_comment", StringType())
    .add("translated_comment", StringType())
    .add("language", StringType())
    .add("country", StringType())
    .add("sentiment", StringType())
)

# Đọc từ Kafka
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.206.110:9092")
    .option("subscribe", "youtube-preprocessed-comments")
    .option("startingOffsets", "earliest")
    .option("kafka.consumer.commit.groupid", "spark-kafka-consumer")
    .option("failOnDataLoss", "false")
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "30000")
    .load()
)

# In dữ liệu thô
raw_query = (
    df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as raw_data")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")  # Trigger mỗi 10 giây
    .start()
)

# Parse JSON
df_parsed = (
    df_raw.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

# In dữ liệu parsed
parsed_query = (
    df_parsed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

# Hàm phân tích cảm xúc
def analyze_sentiment(text):
    if not text or len(text.strip()) < 3:
        print(f"Skipping sentiment analysis for text: {text}")
        return "unknown"
    try:
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        print(f"Text: {text}, Polarity: {polarity}")
        return "positive" if polarity > 0.2 else "negative" if polarity < -0.2 else "neutral"
    except Exception as e:
        print(f"Error analyzing sentiment for text '{text}': {e}")
        return "unknown"

# Đăng ký UDF
sentiment_udf = udf(analyze_sentiment, StringType())

# Áp dụng sentiment
df_with_sentiment = df_parsed.withColumn("sentiment", sentiment_udf(col("translated_comment")))

# In dữ liệu sau sentiment
sentiment_query = (
    df_with_sentiment.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

# Chuyển về JSON và ghi vào Kafka
df_output = df_with_sentiment.select(
    to_json(
        struct(
            col("original_comment"),
            col("cleaned_comment"),
            col("translated_comment"),
            col("language"),
            col("country"),
            col("sentiment")
        )
    ).alias("value")
)

# Ghi vào Kafka
query = (
    df_output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.206.110:9092")
    .option("topic", "youtube-sentiment-results")
    .option("checkpointLocation", "/tmp/sentiment-checkpoint")
    .option("kafka.producer.acks", "all")
    .option("kafka.request.timeout.ms", "60000")
    .trigger(processingTime="10 seconds")
    .outputMode("append")
    .start()
)
