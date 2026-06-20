from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, pandas_udf
from pyspark.sql.types import FloatType
import pandas as pd
import pickle
import sys
import os

# Import cấu hình
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import TRANSACTION_SCHEMA
from config.db_config import DB_URL, DB_DRIVER, DB_TABLE, DB_USER, DB_PASS
from config.kafka_config import KAFKA_BROKER, TRANSACTION_TOPIC

# Load bộ chuẩn hóa và mô hình XGBoost
with open('../../models/scaler.pkl', 'rb') as f:
    scaler = pickle.load(f)
with open('../../models/fraud_model.pkl', 'rb') as f:
    model = pickle.load(f)

# UDF để dự đoán bằng Pandas & Sklearn/XGBoost
@pandas_udf(FloatType())
def predict_fraud(features: pd.DataFrame) -> pd.Series:
    # Scale 30 cột features (Time, V1->V28, Amount) như lúc train
    scaled_features = scaler.transform(features)
    # Lấy xác suất của Class 1 (Gian lận)
    probabilities = model.predict_proba(scaled_features)[:, 1]
    return pd.Series(probabilities)

def write_to_postgres(df, epoch_id):
    # Chỉ lưu các giao dịch có xác suất gian lận > 70%
    fraud_df = df.filter(col("fraud_probability") > 0.70)
    output_df = fraud_df.select("Time", "Amount", "Class", "fraud_probability")
    
    output_df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("driver", DB_DRIVER) \
        .option("dbtable", DB_TABLE) \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .mode("append") \
        .save()

def start_streaming():
    # Khởi tạo Spark Session với package Kafka và PostgreSQL
    spark = SparkSession.builder \
        .appName("FraudDetectorStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Đọc luồng dữ liệu từ Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TRANSACTION_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON
    parsed_df = df.select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")).select("data.*")

    # Chỉ định 30 cột features đưa vào mô hình (khớp với quá trình train)
    feature_cols = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]
    
    # Tạo cột xác suất gian lận
    predictions_df = parsed_df.withColumn(
        "fraud_probability",
        predict_fraud(struct(*[col(c) for c in feature_cols]))
    )

    # Ghi cảnh báo vào PostgreSQL
    db_query = predictions_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .start()

    # In ra console để theo dõi trực tiếp
    console_query = predictions_df.select("Time", "Amount", "Class", "fraud_probability").writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    start_streaming()