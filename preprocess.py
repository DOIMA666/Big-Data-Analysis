import json
import re
import emoji
from confluent_kafka import Consumer, Producer, KafkaError
from langdetect import detect, LangDetectException
from googletrans import Translator
import logging
import time
from functools import lru_cache

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = "192.168.206.110:9092"
INPUT_TOPIC = "youtube-comments-stream"
OUTPUT_TOPIC = "youtube-preprocessed-comments"

# Khởi tạo Translator
translator = Translator()

# Bộ nhớ đệm cho dịch thuật
@lru_cache(maxsize=1000)
def cached_translate(text, dest="en"):
    """Dịch văn bản với bộ nhớ đệm để tối ưu hóa."""
    try:
        return translator.translate(text, dest=dest).text
    except Exception as e:
        logging.error(f"Translation error for text '{text}': {e}")
        return text

def delivery_report(err, msg):
    """Báo cáo trạng thái gửi message tới Kafka."""
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def clean_text(text):
    """Làm sạch văn bản."""
    if not isinstance(text, str) or len(text.strip()) == 0:
        return None
    
    # Chuyển đổi emoji thành văn bản
    text = emoji.demojize(text)
    
    # Loại bỏ URL
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    
    # Loại bỏ ký tự đặc biệt, chỉ giữ chữ và số
    text = re.sub(r'[^\w\s]', '', text)
    
    # Chuyển về chữ thường và loại bỏ khoảng trắng thừa
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)
    
    # Loại bỏ các bình luận quá ngắn hoặc chỉ chứa số
    if len(text) < 3 or text.isdigit():
        return None
    
    return text

def detect_language(text):
    """Phát hiện ngôn ngữ của văn bản."""
    try:
        if not isinstance(text, str) or len(text.strip()) < 3:
            return 'unknown'
        return detect(text)
    except LangDetectException:
        logging.warning(f"Could not detect language for text: {text}")
        return 'unknown'

def map_language_to_country(lang, comment):
    """Ánh xạ ngôn ngữ sang quốc gia."""
    language_to_country = {
        'es': 'Spain',
        'en': 'United States',
        'bn': 'Bangladesh',
        'tr': 'Turkey',
        'ar': 'Saudi Arabia',
        'ru': 'Russia',
        'unknown': 'Unknown'
    }
    # Kiểm tra nội dung bình luận để tìm thông tin quốc gia
    if isinstance(comment, str):
        comment = comment.lower()
        if 'colombia' in comment:
            return 'Colombia'
        elif 'punjab' in comment:
            return 'India'
    return language_to_country.get(lang, 'Unknown')

def preprocess_comment(comment_data):
    """Tiền xử lý một bình luận."""
    try:
        comment = comment_data.get("comment", "")
        sentiment = comment_data.get("sentiment", "unknown")
        
        # Làm sạch văn bản
        cleaned = clean_text(comment)
        if not cleaned:
            logging.warning(f"Comment skipped due to invalid content: {comment}")
            return None
        
        # Phát hiện ngôn ngữ
        language = detect_language(comment)
        
        # Ánh xạ quốc gia
        country = map_language_to_country(language, comment)
        
        # Dịch sang tiếng Anh nếu không phải tiếng Anh
        translated = cleaned
        if language != "en" and language != "unknown":
            translated = cached_translate(cleaned)
            if not translated or len(translated) < 3:
                logging.warning(f"Translated comment too short or invalid: {translated}")
                return None
        
        # Tạo dictionary kết quả
        result = {
            "original_comment": comment,
            "cleaned_comment": cleaned,
            "translated_comment": translated,
            "language": language,
            "country": country,
            "sentiment": sentiment
        }
        
        return result
    
    except Exception as e:
        logging.error(f"Error processing comment '{comment}': {e}")
        return None

def main():
    # Cấu hình Kafka Consumer
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "comment-preprocessor-group",
        "auto.offset.reset": "earliest"
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([INPUT_TOPIC])

    # Cấu hình Kafka Producer
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "comment-preprocessor"
    }
    producer = Producer(producer_conf)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                    break

            # Parse message
            try:
                data = json.loads(msg.value().decode("utf-8"))
                processed = preprocess_comment(data)
                
                if processed:
                    # Gửi dữ liệu đã xử lý tới topic mới
                    producer.produce(
                        OUTPUT_TOPIC,
                        value=json.dumps(processed).encode("utf-8"),
                        callback=delivery_report
                    )
                    producer.poll(0)
            except Exception as e:
                logging.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logging.info("Received KeyboardInterrupt, shutting down...")
    finally:
        logging.info("Closing consumer and producer...")
        consumer.close()
        producer.flush()
        producer.close()
        logging.info("Shutdown complete.")

if __name__ == "__main__":
    main()