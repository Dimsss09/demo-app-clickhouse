CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

CREATE TABLE IF NOT EXISTS kafka_orders_raw
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'shop.public.orders',
    kafka_group_name = 'ch_orders_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS fact_orders
(
    order_id UInt64,
    customer_id UInt64,
    status LowCardinality(String),
    total_amount Decimal(12, 2),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _version UInt64,
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS agg_revenue_per_minute
(
    minute DateTime,
    order_count UInt64,
    revenue Decimal(18, 2)
)
ENGINE = SummingMergeTree
ORDER BY minute;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orders_to_fact_orders
TO fact_orders AS
SELECT
    JSONExtractUInt(message, 'after', 'id') AS order_id,
    JSONExtractUInt(message, 'after', 'customer_id') AS customer_id,
    JSONExtractString(message, 'after', 'status') AS status,
    toDecimal64OrZero(JSONExtractString(message, 'after', 'total_amount'), 2) AS total_amount,
    fromUnixTimestamp64Milli(JSONExtractUInt(message, 'after', 'created_at')) AS created_at,
    fromUnixTimestamp64Milli(JSONExtractUInt(message, 'after', 'updated_at')) AS updated_at,
    if(JSONExtractUInt(message, 'source', 'lsn') = 0, JSONExtractUInt(message, 'ts_ms'), JSONExtractUInt(message, 'source', 'lsn')) AS _version,
    0 AS is_deleted
FROM kafka_orders_raw
WHERE JSONExtractString(message, 'op') IN ('c', 'r');

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fact_orders_to_revenue_per_minute
TO agg_revenue_per_minute AS
SELECT
    toStartOfMinute(created_at) AS minute,
    count() AS order_count,
    sum(total_amount) AS revenue
FROM fact_orders
WHERE status = 'paid'
GROUP BY minute;
