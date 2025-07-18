import os
from googleapiclient.discovery import build
from confluent_kafka import Producer
import json
import time
import emoji

# Cấu hình YouTube API
API_KEY = "AIzaSyCN_gHvEg4n7v4LxnS5X474eDJz5AAyUAw"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
VIDEO_ID = "uyiG6uw-6pA"

# Cấu hình Kafka Producer
KAFKA_BOOTSTRAP_SERVERS = "192.168.206.110:9092"
KAFKA_TOPIC = "youtube-comments-stream"

def delivery_report(err, msg):
    """Báo cáo trạng thái gửi message tới Kafka."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def get_youtube_comments(youtube, video_id):
    """Lấy bình luận từ YouTube API."""
    comments = []
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=100,  # Số lượng bình luận tối đa mỗi request
        textFormat="plainText"
    )
    
    while request:
        response = request.execute()
        for item in response["items"]:
            comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            # Loại bỏ emoji để tránh lỗi encoding
            comment = emoji.replace_emoji(comment, replace="")
            comments.append(comment)
        
        # Chuyển sang trang tiếp theo nếu có
        request = youtube.commentThreads().list_next(request, response)
    
    return comments

def main():
    # Khởi tạo YouTube API client
    youtube = build(
        YOUTUBE_API_SERVICE_NAME,
        YOUTUBE_API_VERSION,
        developerKey=API_KEY
    )
    
    # Khởi tạo Kafka Producer
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "youtube-comment-producer"
    }
    producer = Producer(producer_conf)
    
    # Lấy bình luận từ YouTube
    print(f"Fetching comments for video ID: {VIDEO_ID}")
    comments = get_youtube_comments(youtube, VIDEO_ID)
    
    # Gửi bình luận tới Kafka
    for comment in comments:
        try:
            producer.produce(
                KAFKA_TOPIC,
                value=json.dumps({"comment": comment}).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)  # Kích hoạt callback
            time.sleep(0.1)  # Tránh gửi quá nhanh
        except Exception as e:
            print(f"Error producing message: {e}")
    
    # Đợi tất cả message được gửi
    producer.flush()
    print(f"Sent {len(comments)} comments to Kafka topic {KAFKA_TOPIC}")

if __name__ == "__main__":
    main()