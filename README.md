# Big-Data-Analysis

# 📊 YouTube Comment Sentiment Analysis with Big Data Pipeline

This is a final project for the Big Data Analytics course, focused on building a data lakehouse pipeline to analyze sentiments from YouTube comments using modern big data tools. The project simulates real-time sentiment classification using streaming technologies and machine learning.

## 📌 Project Overview

The main goal is to analyze public sentiment from YouTube comments by building an end-to-end data pipeline. This includes data ingestion, processing, machine learning classification, and storage using scalable big data technologies.

We simulate real-time comment ingestion and perform sentiment analysis using a logistic regression model, then store and visualize the results in Cassandra and a dashboard.

## 🎯 Objectives

- Build a real-time big data pipeline from ingestion to analytics.
- Apply machine learning to classify comment sentiments (positive, negative, neutral).
- Store processed results in a distributed NoSQL database.
- Visualize sentiment trends over time.

---

## 🛠️ Technologies Used

| Layer            | Tool                     | Description                                      |
|------------------|--------------------------|--------------------------------------------------|
| Data Source       | CSV (YouTube comments)   | Preprocessed and translated sentiment dataset    |
| Ingestion         | Apache Kafka             | Simulates streaming comment input                |
| Processing        | Apache Spark Streaming   | Real-time sentiment analysis with MLlib          |
| Machine Learning  | Logistic Regression      | Sentiment classification model                   |
| Storage           | Apache Cassandra         | Stores sentiment results for querying/visualizing|
| Visualization     | Apache Superset / Grafana| (Optional) Real-time sentiment dashboard         |

---

## 🗂️ System Architecture

```

```
        +-------------------+
        |   CSV Data File   |
        +--------+----------+
                 |
                 v
     +-----------+------------+
     | Apache Kafka (Producer)|
     +-----------+------------+
                 |
                 v
  +--------------+----------------+
  | Apache Spark Streaming (MLlib)|
  |  - Load model                 |
  |  - Predict sentiment          |
  +--------------+----------------+
                 |
                 v
     +-----------+-----------+
     |    Apache Kafka       |
     |     (Result topic)    |
     +-----------+-----------+
                 |
                 v
     +-----------+-----------+
     |  Apache Cassandra DB  |
     +-----------------------+
```

```

---

## 📁 Project Structure

```

project-root/
├── data/
│   └── youtube\_comments\_clean.csv
├── kafka/
│   ├── kafka\_producer.py
│   └── kafka\_consumer.py
├── spark/
│   └── spark\_sentiment\_pipeline.py
├── model/
│   └── sentiment\_model.pkl
├── cassandra/
│   └── schema\_setup.cql
├── dashboard/
│   └── (Optional Superset/Grafana config)
└── README.md

````

---

## ⚙️ How to Run

### 1. Start Required Services

Make sure the following services are running:
- Apache Kafka
- Apache Spark
- Apache Cassandra

You can use Docker Compose or install them manually.

### 2. Train the ML Model (Optional)

If needed, run the script to train and export the sentiment model:

```bash
python train_model.py
````

It will create `sentiment_model.pkl` used by Spark.

### 3. Start Kafka Producer

Send simulated comments to Kafka topic:

```bash
python kafka_producer.py
```

This reads from `youtube_comments_clean.csv` and streams data to `youtube-training-data` topic.

### 4. Start Spark Streaming Job

```bash
spark-submit spark_sentiment_pipeline.py
```

This script loads the model and classifies incoming comments, sending results to Kafka or Cassandra.

### 5. View Results in Cassandra

After streaming completes, query Cassandra:

```cql
SELECT * FROM youtube.sentiment_results;
```

### 6. (Optional) Visualize Sentiments

You can connect Superset or Grafana to Cassandra to build a dashboard showing:

* Sentiment over time
* Top categories with negative/positive feedback
* Comment volume trends

---

## 📚 Authors

* Lê Quang Hoàng Phát
* Lê Quỳnh Nhựt Vinh
* Trần Bảo Việt

Supervisor: **Lê Thị Minh Châu**, M.S.
Ho Chi Minh City University of Technology and Education (HCMUTE)

---

## 📝 License

This project is for educational purposes only.

```

