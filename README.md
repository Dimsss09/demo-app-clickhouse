# RealtimeShop Analytics

**Real-Time E-Commerce Analytics Platform** — pipeline OLTP → OLAP berbasis Change Data Capture (CDC).

Data transaksi dari aplikasi e-commerce (PostgreSQL) direplikasi secara _near real-time_ ke ClickHouse menggunakan Debezium + Kafka, lalu diubah menjadi tabel analitik (fact, dimension, dan agregasi) untuk kebutuhan analitik berlatensi rendah: revenue, produk terlaris, payment success rate, pergerakan stok, dan order stream real-time.

> Ringkasan CV (EN):
> Built a real-time OLTP-to-OLAP analytics pipeline using Spring Boot, PostgreSQL logical replication, Debezium, Kafka, and ClickHouse. Implemented CDC-based synchronization from transactional tables into analytical fact tables with ClickHouse Kafka Engine and Materialized Views for low-latency sales, payment, and product analytics.

---

## Arsitektur

```
Spring Boot Data Generator / API
            │
            ▼
   PostgreSQL (OLTP)
            │  logical replication (WAL)
            ▼
   Debezium (Kafka Connect)
            │
            ▼
       Kafka Topics
   shop.public.orders
   shop.public.order_items
   shop.public.payments
   shop.public.products
   shop.public.customers
   shop.public.inventory_events
            │
            ▼
 ClickHouse Kafka Engine Tables (raw)
            │  Materialized Views
            ▼
 ClickHouse OLAP Tables
   fact_orders / fact_payments / fact_inventory_events
   dim_customers / dim_products
   agg_revenue_per_minute / agg_product_sales_daily / agg_payment_status_hourly
            │
            ▼
   Dashboard / Query API (Grafana / Metabase / REST)
```

## Tech Stack

| Layer            | Teknologi                                             |
| ---------------- | ----------------------------------------------------- |
| OLTP / Source    | PostgreSQL (logical replication)                      |
| Data Generator   | Spring Boot (Java)                                    |
| CDC              | Debezium (Kafka Connect)                              |
| Event Backbone   | Apache Kafka                                          |
| OLAP / Warehouse | ClickHouse (Kafka Engine + Materialized Views)        |
| Orchestration    | Docker Compose                                        |
| Observability    | Kafka UI, ClickHouse system tables, dead-letter topic |
| Visualisasi      | Grafana + ClickHouse datasource                       |

## Dokumentasi

Dokumentasi lengkap ada di folder [`docs/`](./docs):

1. [Architecture](./docs/01-architecture.md) — komponen, aliran data, dan keputusan desain.
2. [Data Model](./docs/02-data-model.md) — skema OLTP (PostgreSQL) dan OLAP (ClickHouse).
3. [CDC Pipeline](./docs/03-cdc-pipeline.md) — setup Debezium, Kafka, ClickHouse Kafka Engine, dan Materialized Views.
4. [Roadmap (MVP)](./docs/04-roadmap.md) — pembagian scope MVP 1, 2, 3.
5. [Analytics Queries](./docs/05-analytics-queries.md) — contoh query analitik untuk demo.
6. [Reliability & Observability](./docs/06-reliability-observability.md) — Kafka UI, DLQ, monitoring lag, dan Grafana dashboard.

## Quick Start MVP 3 — Reliability & Observability

```powershell
# 1. Jalankan seluruh stack observability + pipeline
docker compose up -d

# 2. Daftarkan / update Debezium connector
.\scripts\register-connector.ps1

# 3. Jalankan generator data
docker compose --profile generator up -d --build generator

# 4. Cek data order masuk ke ClickHouse
docker exec -it shop-clickhouse clickhouse-client `
  -u clickhouse --password clickhouse --database analytics `
  --query "SELECT count() FROM fact_orders"

# 5. Cek agregasi revenue per menit
docker exec -it shop-clickhouse clickhouse-client `
  -u clickhouse --password clickhouse --database analytics `
  --query "SELECT * FROM agg_revenue_per_minute ORDER BY minute DESC LIMIT 10"
```

Endpoint demo:

| Service            | URL / akses             | Keterangan                                                    |
| ------------------ | ----------------------- | ------------------------------------------------------------- |
| Kafka UI           | <http://localhost:8080> | Topic, messages, Kafka Connect, consumer lag.                 |
| Kafka Connect REST | <http://localhost:8083> | Status connector/task Debezium.                               |
| ClickHouse HTTP    | <http://localhost:8123> | Query HTTP ke database `analytics`.                           |
| Grafana            | <http://localhost:3000> | Login `admin` / `admin`, dashboard **RealtimeShop Overview**. |

## Demo Reliability & Observability

### Cek status connector Debezium

```powershell
curl http://localhost:8083/connectors/shop-postgres-connector/status
```

### Cek dead-letter topic

Connector sudah dikonfigurasi dengan `errors.tolerance=all` dan DLQ `shop.dlq`.

```powershell
docker exec -it shop-kafka kafka-console-consumer `
  --bootstrap-server kafka:9092 `
  --topic shop.dlq `
  --from-beginning `
  --max-messages 10
```

### Cek consumer lag / ingest ClickHouse

```powershell
docker exec -it shop-clickhouse clickhouse-client `
  -u clickhouse --password clickhouse --database analytics `
  --query "SELECT database, table, consumer_id, last_poll_time, num_messages_read, last_exception FROM system.kafka_consumers ORDER BY table"
```

```powershell
docker exec -it shop-clickhouse clickhouse-client `
  -u clickhouse --password clickhouse --database analytics `
  --query "SELECT table, sum(rows) AS rows, count() AS parts FROM system.parts WHERE active AND database='analytics' GROUP BY table ORDER BY rows DESC"
```

### Buka dashboard Grafana

1. Buka <http://localhost:3000>.
2. Login `admin` / `admin`.
3. Buka folder **RealtimeShop** → dashboard **RealtimeShop Overview**.
4. Pastikan panel revenue, payment success rate, top products, dan live order stream berubah saat generator berjalan.

## Quick Start alternatif via curl

```bash
# 1. Jalankan seluruh stack
docker compose up -d

# 2. Daftarkan Debezium connector ke PostgreSQL
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/postgres-connector.json

# 3. Jalankan data generator (Spring Boot)
docker compose --profile generator up -d --build generator

# 4. Cek hasil analitik di ClickHouse
docker exec -it shop-clickhouse clickhouse-client \
  -u clickhouse --password clickhouse --database analytics \
  --query "SELECT * FROM agg_revenue_per_minute ORDER BY minute DESC LIMIT 10"

# 5. Buka dashboard / Kafka UI
#    Kafka UI:    http://localhost:8080
#    Grafana:     http://localhost:3000
```

## Status

✅ MVP 1–3 implemented secara lokal: CDC pipeline, analytical modeling, reliability/observability, Kafka UI, DLQ, dan Grafana dashboard. Runtime verification tetap bergantung pada Docker daemon dan image pull lokal.
