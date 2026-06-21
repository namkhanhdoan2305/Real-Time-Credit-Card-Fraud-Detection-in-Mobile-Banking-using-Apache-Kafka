import os
import sys
import json
import pickle

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from sqlalchemy import create_engine


# =========================
# Windows Spark config
# =========================
os.environ["HADOOP_HOME"] = "C:\\Hadoop\\hadoop-3.3.0"
os.environ["PATH"] += os.pathsep + "C:\\Hadoop\\hadoop-3.3.0\\bin"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


# =========================
# Import config
# =========================
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import TRANSACTION_SCHEMA
from config.kafka_config import KAFKA_BROKER, TRANSACTION_TOPIC
from config.db_config import (
    DB_USER,
    DB_PASS,
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_TABLE,
    FRAUD_ALERT_TABLE,
)


# =========================
# Paths
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MODEL_PATH = os.path.join(BASE_DIR, "models", "fraud_model.pkl")
SCALER_PATH = os.path.join(BASE_DIR, "models", "scaler.pkl")
THRESHOLD_PATH = os.path.join(BASE_DIR, "models", "threshold.json")

FEATURE_COLS = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]


# =========================
# Load model
# =========================
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"Không tìm thấy model: {MODEL_PATH}")

if not os.path.exists(SCALER_PATH):
    raise FileNotFoundError(f"Không tìm thấy scaler: {SCALER_PATH}")

with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

with open(SCALER_PATH, "rb") as f:
    scaler = pickle.load(f)

if os.path.exists(THRESHOLD_PATH):
    with open(THRESHOLD_PATH, "r", encoding="utf-8") as f:
        threshold_data = json.load(f)
        THRESHOLD = float(threshold_data.get("threshold", 0.70))
else:
    THRESHOLD = 0.70

print(f"Loaded model: {MODEL_PATH}")
print(f"Loaded scaler: {SCALER_PATH}")
print(f"Threshold: {THRESHOLD}")


# =========================
# PostgreSQL engine
# =========================
POSTGRES_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(POSTGRES_URL, pool_pre_ping=True)


def get_alert_level(risk_score: int) -> str:
    if risk_score >= 90:
        return "CRITICAL"
    if risk_score >= 80:
        return "HIGH"
    if risk_score >= 70:
        return "MEDIUM"
    return "LOW"


def process_batch(batch_df, epoch_id):
    """
    Hàm này chạy mỗi micro-batch.
    Spark đọc Kafka xong, convert batch sang Pandas, predict bằng model,
    rồi ghi transactions + fraud_alerts vào PostgreSQL.
    """

    if batch_df.rdd.isEmpty():
        return

    pdf = batch_df.toPandas()

    if pdf.empty:
        return

    # Đảm bảo đủ feature
    missing_cols = [c for c in FEATURE_COLS if c not in pdf.columns]
    if missing_cols:
        print(f"Batch {epoch_id} thiếu cột: {missing_cols}")
        return

    # Convert numeric
    for c in FEATURE_COLS:
        pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

    pdf = pdf.dropna(subset=FEATURE_COLS)

    if pdf.empty:
        return

    # Predict
    X = pdf[FEATURE_COLS].astype(float)
    X_scaled = scaler.transform(X)

    fraud_prob = model.predict_proba(X_scaled)[:, 1]

    pdf["fraud_probability"] = fraud_prob
    pdf["risk_score"] = (pdf["fraud_probability"] * 100).astype(int)

    pdf["status"] = pdf["fraud_probability"].apply(
        lambda p: "REJECTED" if p >= THRESHOLD else "APPROVED"
    )

    pdf["reason"] = pdf["fraud_probability"].apply(
        lambda p: "HIGH_FRAUD_PROBABILITY" if p >= THRESHOLD else "LOW_RISK"
    )

    pdf["alert_level"] = pdf["risk_score"].apply(get_alert_level)

    # Chọn cột ghi transactions
    transaction_cols = [
        "transaction_id",
        "customer_id",
        "account_id",
        "device_id",
        "Time",
        "Amount",
        "Class",
        "transaction_type",
        "merchant_id",
        "location",
        "channel",
        "fraud_probability",
        "risk_score",
        "status",
        "reason",
        "event_time",
        "detected_at",
    ]

    existing_transaction_cols = [c for c in transaction_cols if c in pdf.columns]

    transactions_out = pdf[existing_transaction_cols].copy()

    transactions_out.to_sql(
        DB_TABLE,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    # Ghi fraud alerts
    alerts_out = pdf[pdf["status"] == "REJECTED"].copy()

    if not alerts_out.empty:
        alert_cols = [
            "transaction_id",
            "customer_id",
            "account_id",
            "device_id",
            "Time",
            "Amount",
            "Class",
            "fraud_probability",
            "risk_score",
            "alert_level",
            "reason",
            "event_time",
            "detected_at",
        ]

        existing_alert_cols = [c for c in alert_cols if c in alerts_out.columns]

        alerts_out[existing_alert_cols].to_sql(
            FRAUD_ALERT_TABLE,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

    print(
        f"Epoch {epoch_id}: inserted {len(transactions_out)} transactions, "
        f"{len(alerts_out)} alerts"
    )


def start_streaming():
    spark = (
        SparkSession.builder
        .appName("FraudDetectorStreaming")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.postgresql:postgresql:42.6.0",
        )
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TRANSACTION_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 500)
        .load()
    )

    parsed_df = (
        raw_df.select(
            from_json(
                col("value").cast("string"),
                TRANSACTION_SCHEMA,
            ).alias("data")
        )
        .select("data.*")
    )

    query = (
        parsed_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option(
            "checkpointLocation",
            os.path.join(BASE_DIR, "checkpoints", "fraud_detector_foreach_batch"),
        )
        .trigger(processingTime="3 seconds")
        .start()
    )

    print("Fraud detector streaming started.")
    print(f"Kafka broker: {KAFKA_BROKER}")
    print(f"Topic: {TRANSACTION_TOPIC}")
    print(f"Writing to PostgreSQL table: {DB_TABLE}")

    query.awaitTermination()


if __name__ == "__main__":
    start_streaming()