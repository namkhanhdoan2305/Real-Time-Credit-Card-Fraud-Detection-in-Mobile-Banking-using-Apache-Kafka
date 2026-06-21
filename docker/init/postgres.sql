CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,

    transaction_id VARCHAR(100),
    customer_id VARCHAR(100),
    account_id VARCHAR(100),
    device_id VARCHAR(100),

    "Time" DOUBLE PRECISION,
    "Amount" DOUBLE PRECISION,
    "Class" INTEGER,

    transaction_type VARCHAR(50),
    merchant_id VARCHAR(100),
    location VARCHAR(100),
    channel VARCHAR(50),

    fraud_probability DOUBLE PRECISION,
    risk_score INTEGER,
    status VARCHAR(30),
    reason VARCHAR(100),

    event_time VARCHAR(100),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    id SERIAL PRIMARY KEY,

    transaction_id VARCHAR(100),
    customer_id VARCHAR(100),
    account_id VARCHAR(100),
    device_id VARCHAR(100),

    "Time" DOUBLE PRECISION,
    "Amount" DOUBLE PRECISION,
    "Class" INTEGER,

    fraud_probability DOUBLE PRECISION,
    risk_score INTEGER,
    alert_level VARCHAR(30),
    reason VARCHAR(100),

    event_time VARCHAR(100),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);