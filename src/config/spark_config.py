from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("device_id", StringType(), True),

        StructField("channel", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("event_time", StringType(), True),

        StructField("Time", DoubleType(), True),
        *[StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)],
        StructField("Amount", DoubleType(), True),

        StructField("Class", IntegerType(), True),
    ]
)