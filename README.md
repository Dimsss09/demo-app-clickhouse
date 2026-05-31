# RealtimeShop Analytics

**Real-Time E-Commerce Analytics Platform** — pipeline OLTP → OLAP berbasis Change Data Capture (CDC).

Data transaksi dari aplikasi e-commerce (PostgreSQL) direplikasi secara *near real-time* ke ClickHouse menggunakan Debezium + Kafka, lalu diubah menjadi tabel analitik (fact, dimension, dan agregasi) untuk kebutuhan analitik berlatensi rendah: revenue, produk terlaris, payment success rate, pergerakan stok, dan order stream real-time.

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

| Layer | Teknologi |
|-------|-----------|
| OLTP / Source | PostgreSQL (logical replication) |
| Data Generator | Spring Boot (Java) |
| CDC | Debezium (Kafka Connect) |
| Event Backbone | Apache Kafka |
| OLAP / Warehouse | ClickHouse (Kafka Engine + Materialized Views) |
| Orchestration | Docker Compose |
| Observability | Kafka UI, ClickHouse system tables, dead-letter topic |
| Visualisasi | Grafana / Metabase / Superset (opsional) |

## Dokumentasi

Dokumentasi lengkap ada di folder [`docs/`](./docs):

1. [Architecture](./docs/01-architecture.md) — komponen, aliran data, dan keputusan desain.
2. [Data Model](./docs/02-data-model.md) — skema OLTP (PostgreSQL) dan OLAP (ClickHouse).
3. [CDC Pipeline](./docs/03-cdc-pipeline.md) — setup Debezium, Kafka, ClickHouse Kafka Engine, dan Materialized Views.
4. [Roadmap (MVP)](./docs/04-roadmap.md) — pembagian scope MVP 1, 2, 3.
5. [Analytics Queries](./docs/05-analytics-queries.md) — contoh query analitik untuk demo.

## Quick Start MVP 1

```powershell
# 1. Jalankan infra dasar
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

Kafka UI tersedia di <http://localhost:8080>, Kafka Connect di <http://localhost:8083>, dan ClickHouse HTTP di <http://localhost:8123>.

## Quick Start (target akhir)

```bash
# 1. Jalankan seluruh stack
docker compose up -d

# 2. Daftarkan Debezium connector ke PostgreSQL
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/postgres-connector.json

# 3. Jalankan data generator (Spring Boot)
#    menghasilkan order, payment, dan perubahan stok terus-menerus

# 4. Cek hasil analitik di ClickHouse
docker exec -it clickhouse clickhouse-client \
  --query "SELECT * FROM agg_revenue_per_minute ORDER BY minute DESC LIMIT 10"

# 5. Buka dashboard / Kafka UI
#    Kafka UI:    http://localhost:8080
#    Grafana:     http://localhost:3000
```

> Catatan: layanan di atas adalah target arsitektur akhir. Lihat [Roadmap](./docs/04-roadmap.md) untuk urutan pembangunannya secara bertahap.

## Status

🚧 Project tahap awal — dokumentasi & desain. Implementasi mengikuti roadmap MVP.
