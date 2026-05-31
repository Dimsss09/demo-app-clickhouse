CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

CREATE TABLE IF NOT EXISTS kafka_customers_raw
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'shop.public.customers',
    kafka_group_name = 'ch_customers_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS kafka_products_raw
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'shop.public.products',
    kafka_group_name = 'ch_products_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

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

CREATE TABLE IF NOT EXISTS kafka_order_items_raw
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'shop.public.order_items',
    kafka_group_name = 'ch_order_items_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS kafka_payments_raw
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'shop.public.payments',
    kafka_group_name = 'ch_payments_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS kafka_inventory_events_raw
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'shop.public.inventory_events',
    kafka_group_name = 'ch_inventory_events_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS dim_customers
(
    id UInt64,
    email String,
    full_name String,
    country String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _version UInt64,
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE TABLE IF NOT EXISTS dim_products
(
    id UInt64,
    sku String,
    name String,
    category String,
    price Decimal(12, 2),
    stock_qty Int32,
    updated_at DateTime64(3),
    _version UInt64,
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

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

CREATE TABLE IF NOT EXISTS fact_order_items
(
    order_item_id UInt64,
    order_id UInt64,
    product_id UInt64,
    quantity UInt32,
    unit_price Decimal(12, 2),
    revenue Decimal(18, 2),
    _version UInt64,
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY order_item_id;

CREATE TABLE IF NOT EXISTS fact_payments
(
    payment_id UInt64,
    order_id UInt64,
    method LowCardinality(String),
    status LowCardinality(String),
    amount Decimal(12, 2),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _version UInt64,
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY payment_id;

CREATE TABLE IF NOT EXISTS fact_inventory_events
(
    event_id UInt64,
    product_id UInt64,
    change_qty Int32,
    reason LowCardinality(String),
    created_at DateTime64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (product_id, created_at);

CREATE TABLE IF NOT EXISTS agg_revenue_per_minute
(
    minute DateTime,
    order_count UInt64,
    revenue Decimal(18, 2)
)
ENGINE = SummingMergeTree
ORDER BY minute;

CREATE TABLE IF NOT EXISTS agg_product_sales_daily
(
    day Date,
    product_id UInt64,
    units_sold UInt64,
    revenue Decimal(18, 2)
)
ENGINE = SummingMergeTree
ORDER BY (day, product_id);

CREATE TABLE IF NOT EXISTS agg_payment_status_hourly
(
    hour DateTime,
    status LowCardinality(String),
    payment_count UInt64,
    total_amount Decimal(18, 2)
)
ENGINE = SummingMergeTree
ORDER BY (hour, status);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customers_to_dim_customers
TO dim_customers AS
SELECT
    if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'id'), JSONExtractUInt(message, 'after', 'id')) AS id,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'email'), JSONExtractString(message, 'after', 'email')) AS email,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'full_name'), JSONExtractString(message, 'after', 'full_name')) AS full_name,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'country'), JSONExtractString(message, 'after', 'country')) AS country,
    fromUnixTimestamp64Milli(if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'created_at'), JSONExtractUInt(message, 'after', 'created_at'))) AS created_at,
    fromUnixTimestamp64Milli(if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'updated_at'), JSONExtractUInt(message, 'after', 'updated_at'))) AS updated_at,
    if(JSONExtractUInt(message, 'source', 'lsn') = 0, JSONExtractUInt(message, 'ts_ms'), JSONExtractUInt(message, 'source', 'lsn')) AS _version,
    if(JSONExtractString(message, 'op') = 'd', 1, 0) AS is_deleted
FROM kafka_customers_raw
WHERE JSONExtractString(message, 'op') IN ('c', 'u', 'd', 'r');

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_products_to_dim_products
TO dim_products AS
SELECT
    if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'id'), JSONExtractUInt(message, 'after', 'id')) AS id,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'sku'), JSONExtractString(message, 'after', 'sku')) AS sku,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'name'), JSONExtractString(message, 'after', 'name')) AS name,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'category'), JSONExtractString(message, 'after', 'category')) AS category,
    toDecimal64OrZero(if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'price'), JSONExtractString(message, 'after', 'price')), 2) AS price,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractInt(message, 'before', 'stock_qty'), JSONExtractInt(message, 'after', 'stock_qty')) AS stock_qty,
    fromUnixTimestamp64Milli(if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'updated_at'), JSONExtractUInt(message, 'after', 'updated_at'))) AS updated_at,
    if(JSONExtractUInt(message, 'source', 'lsn') = 0, JSONExtractUInt(message, 'ts_ms'), JSONExtractUInt(message, 'source', 'lsn')) AS _version,
    if(JSONExtractString(message, 'op') = 'd', 1, 0) AS is_deleted
FROM kafka_products_raw
WHERE JSONExtractString(message, 'op') IN ('c', 'u', 'd', 'r');

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orders_to_fact_orders
TO fact_orders AS
SELECT
    if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'id'), JSONExtractUInt(message, 'after', 'id')) AS order_id,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'customer_id'), JSONExtractUInt(message, 'after', 'customer_id')) AS customer_id,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'status'), JSONExtractString(message, 'after', 'status')) AS status,
    toDecimal64OrZero(if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'total_amount'), JSONExtractString(message, 'after', 'total_amount')), 2) AS total_amount,
    fromUnixTimestamp64Milli(if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'created_at'), JSONExtractUInt(message, 'after', 'created_at'))) AS created_at,
    fromUnixTimestamp64Milli(if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'updated_at'), JSONExtractUInt(message, 'after', 'updated_at'))) AS updated_at,
    if(JSONExtractUInt(message, 'source', 'lsn') = 0, JSONExtractUInt(message, 'ts_ms'), JSONExtractUInt(message, 'source', 'lsn')) AS _version,
    if(JSONExtractString(message, 'op') = 'd', 1, 0) AS is_deleted
FROM kafka_orders_raw
WHERE JSONExtractString(message, 'op') IN ('c', 'u', 'd', 'r');

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_order_items_to_fact_order_items
TO fact_order_items AS
SELECT
    if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'id'), JSONExtractUInt(message, 'after', 'id')) AS order_item_id,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'order_id'), JSONExtractUInt(message, 'after', 'order_id')) AS order_id,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'product_id'), JSONExtractUInt(message, 'after', 'product_id')) AS product_id,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'quantity'), JSONExtractUInt(message, 'after', 'quantity')) AS quantity,
    toDecimal64OrZero(if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'unit_price'), JSONExtractString(message, 'after', 'unit_price')), 2) AS unit_price,
    quantity * unit_price AS revenue,
    if(JSONExtractUInt(message, 'source', 'lsn') = 0, JSONExtractUInt(message, 'ts_ms'), JSONExtractUInt(message, 'source', 'lsn')) AS _version,
    if(JSONExtractString(message, 'op') = 'd', 1, 0) AS is_deleted
FROM kafka_order_items_raw
WHERE JSONExtractString(message, 'op') IN ('c', 'u', 'd', 'r');

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_payments_to_fact_payments
TO fact_payments AS
SELECT
    if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'id'), JSONExtractUInt(message, 'after', 'id')) AS payment_id,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'order_id'), JSONExtractUInt(message, 'after', 'order_id')) AS order_id,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'method'), JSONExtractString(message, 'after', 'method')) AS method,
    if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'status'), JSONExtractString(message, 'after', 'status')) AS status,
    toDecimal64OrZero(if(JSONExtractString(message, 'op') = 'd', JSONExtractString(message, 'before', 'amount'), JSONExtractString(message, 'after', 'amount')), 2) AS amount,
    fromUnixTimestamp64Milli(if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'created_at'), JSONExtractUInt(message, 'after', 'created_at'))) AS created_at,
    fromUnixTimestamp64Milli(if(JSONExtractString(message, 'op') = 'd', JSONExtractUInt(message, 'before', 'updated_at'), JSONExtractUInt(message, 'after', 'updated_at'))) AS updated_at,
    if(JSONExtractUInt(message, 'source', 'lsn') = 0, JSONExtractUInt(message, 'ts_ms'), JSONExtractUInt(message, 'source', 'lsn')) AS _version,
    if(JSONExtractString(message, 'op') = 'd', 1, 0) AS is_deleted
FROM kafka_payments_raw
WHERE JSONExtractString(message, 'op') IN ('c', 'u', 'd', 'r');

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_inventory_events_to_fact_inventory_events
TO fact_inventory_events AS
SELECT
    JSONExtractUInt(message, 'after', 'id') AS event_id,
    JSONExtractUInt(message, 'after', 'product_id') AS product_id,
    JSONExtractInt(message, 'after', 'change_qty') AS change_qty,
    JSONExtractString(message, 'after', 'reason') AS reason,
    fromUnixTimestamp64Milli(JSONExtractUInt(message, 'after', 'created_at')) AS created_at
FROM kafka_inventory_events_raw
WHERE JSONExtractString(message, 'op') IN ('c', 'r');

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fact_orders_to_revenue_per_minute
TO agg_revenue_per_minute AS
SELECT
    toStartOfMinute(created_at) AS minute,
    count() AS order_count,
    sum(total_amount) AS revenue
FROM fact_orders
WHERE status = 'paid' AND is_deleted = 0
GROUP BY minute;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_order_items_to_product_sales_daily
TO agg_product_sales_daily AS
SELECT
    today() AS day,
    product_id,
    sum(quantity) AS units_sold,
    sum(revenue) AS revenue
FROM fact_order_items
WHERE is_deleted = 0
GROUP BY day, product_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_payments_to_payment_status_hourly
TO agg_payment_status_hourly AS
SELECT
    toStartOfHour(created_at) AS hour,
    status,
    count() AS payment_count,
    sum(amount) AS total_amount
FROM fact_payments
WHERE is_deleted = 0
GROUP BY hour, status;
