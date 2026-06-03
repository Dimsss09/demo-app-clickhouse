# 06 — Reliability & Observability

Dokumen ini merangkum fitur MVP 3: observability untuk Kafka/Debezium/ClickHouse, dead-letter topic, dan dashboard demo.

---

## 1. Komponen observability

| Komponen           | URL                     | Kegunaan                                                       |
| ------------------ | ----------------------- | -------------------------------------------------------------- |
| Kafka UI           | <http://localhost:8080> | Inspeksi topic, pesan Debezium, consumer group, dan lag.       |
| Kafka Connect REST | <http://localhost:8083> | Cek status connector/task Debezium.                            |
| ClickHouse HTTP    | <http://localhost:8123> | Endpoint query HTTP ClickHouse.                                |
| Grafana            | <http://localhost:3000> | Dashboard real-time dari ClickHouse. Login: `admin` / `admin`. |

Grafana diprovision otomatis dari folder `grafana/provisioning` dan memakai datasource `ClickHouse Analytics` ke database `analytics`.

---

## 2. Dead-letter topic Debezium

Connector PostgreSQL Debezium dikonfigurasi untuk tidak langsung berhenti saat ada event bermasalah:

```json
{
  "errors.tolerance": "all",
  "errors.retry.timeout": "60000",
  "errors.retry.delay.max.ms": "5000",
  "errors.deadletterqueue.topic.name": "shop.dlq",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.log.enable": "true"
}
```

Topic DLQ: `shop.dlq`.

Cara cek lewat CLI:

```powershell
docker exec -it shop-kafka kafka-console-consumer `
  --bootstrap-server kafka:9092 `
  --topic shop.dlq `
  --from-beginning `
  --max-messages 10
```

Cara cek lewat Kafka UI:

1. Buka <http://localhost:8080>.
2. Pilih cluster `realtime-shop`.
3. Buka **Topics** → `shop.dlq`.
4. Cek tab **Messages** dan headers error context.

> Catatan: DLQ Kafka Connect menangkap error yang terjadi di proses connector. Error parsing di sisi ClickHouse perlu dipantau via `system.kafka_consumers` dan log ClickHouse.

---

## 3. Monitoring lag Kafka / ClickHouse

### Kafka UI

Gunakan menu **Consumers** untuk memantau group ClickHouse:

- `ch_customers_consumer`
- `ch_products_consumer`
- `ch_orders_consumer`
- `ch_order_items_consumer`
- `ch_payments_consumer`
- `ch_inventory_events_consumer`

Lag yang sehat untuk demo lokal biasanya kembali mendekati `0` setelah generator berjalan stabil.

### ClickHouse system tables

```sql
SELECT
    database,
    table,
    consumer_id,
    assignments.topic AS topic,
    assignments.partition_id AS partition_id,
    assignments.current_offset AS current_offset,
    assignments.intent_size AS buffered_messages,
    last_poll_time,
    num_messages_read,
    last_exception
FROM system.kafka_consumers
ORDER BY table, consumer_id;
```

Pantau jumlah baris/parts tabel OLAP:

```sql
SELECT
    database,
    table,
    sum(rows) AS rows,
    count() AS active_parts,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk
FROM system.parts
WHERE active AND database = 'analytics'
GROUP BY database, table
ORDER BY rows DESC;
```

Pantau query/materialized view yang error:

```sql
SELECT
    event_time,
    query_kind,
    exception_code,
    exception,
    query
FROM system.query_log
WHERE type = 'ExceptionWhileProcessing'
ORDER BY event_time DESC
LIMIT 20;
```

---

## 4. Dashboard Grafana

Dashboard yang diprovision: **RealtimeShop / RealtimeShop Overview**.

Panel bawaan:

1. Revenue per minute.
2. Payment success rate 24 jam.
3. Active orders.
4. Top products today.
5. Live order stream.

Jalankan stack dan generator:

```powershell
docker compose up -d
.\scripts\register-connector.ps1
docker compose --profile generator up -d --build generator
```

Lalu buka <http://localhost:3000> dan login `admin` / `admin`.

---

## 5. Health check operasional

```powershell
# Status container
docker compose ps

# Status connector Debezium
curl http://localhost:8083/connectors/shop-postgres-connector/status

# Topic Kafka yang terbentuk
docker exec -it shop-kafka kafka-topics --bootstrap-server kafka:9092 --list

# Row count tabel utama
docker exec -it shop-clickhouse clickhouse-client `
  -u clickhouse --password clickhouse --database analytics `
  --query "SELECT 'fact_orders' AS table, count() AS rows FROM fact_orders UNION ALL SELECT 'fact_payments', count() FROM fact_payments"
```
