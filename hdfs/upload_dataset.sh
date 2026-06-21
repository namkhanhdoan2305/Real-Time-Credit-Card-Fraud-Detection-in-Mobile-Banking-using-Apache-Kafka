#!/bin/bash

echo "=========================================="
echo "Upload dataset lên HDFS"
echo "=========================================="

LOCAL_FILE="../dataset/raw/creditcard.csv"
HDFS_PATH="/user/bigdata/fraud_detection/raw/"

if [ ! -f "$LOCAL_FILE" ]; then
    echo "Lỗi: Không tìm thấy file $LOCAL_FILE"
    exit 1
fi

echo "Đang đẩy $LOCAL_FILE lên HDFS tại $HDFS_PATH..."
hdfs dfs -put -f $LOCAL_FILE $HDFS_PATH

echo "Upload hoàn tất! Kiểm tra file trên HDFS:"
hdfs dfs -ls $HDFS_PATH