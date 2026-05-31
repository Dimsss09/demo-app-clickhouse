# 04 — Roadmap (MVP)

Project dibangun bertahap. Setiap MVP menghasilkan sesuatu yang bisa didemokan dan ditulis di CV.

---

## MVP 1 — Basic CDC Pipeline ✅ target pertama

**Tujuan:** membuktikan aliran data end-to-end OLTP → OLAP berfungsi.

- [x] `docker-compose.yml`: PostgreSQL, Kafka, Zookeeper/KRaft, Kafka Connect (Debezium), ClickHouse.
- [x] Skema OLTP PostgreSQL (`customers`, `products`, `orders`, `order_items`, `payments`).
- [x] Aktifkan logical replication + publication.
- [x] Spring Boot generator sederhana: bikin order + payment terus-menerus (insert saja dulu).
- [x] Debezium connector terdaftar, topic muncul di Kafka. *(config + helper registration sudah dibuat; runtime verification menunggu Docker daemon aktif)*
- [x] ClickHouse Kafka Engine table + Materialized View → `fact_orders`.
- [x] Query revenue & order count real-time berjalan. *(query sudah tersedia; runtime verification menunggu Docker daemon aktif)*

**Deliverable demo:** `SELECT count() FROM fact_orders` naik seiring generator jalan.

---

## MVP 2 — Analytical Modeling

**Tujuan:** ubah data mentah jadi model analitik yang berguna.

- [x] Tambah `dim_customers`, `dim_products` (ReplacingMergeTree).
- [x] Tambah `fact_payments`, `fact_inventory_events`.
- [x] Materialized View agregasi:
  - [x] `agg_revenue_per_minute`
  - [x] `agg_product_sales_daily` (top products)
  - [x] `agg_payment_status_hourly` (success rate)
- [x] **Handling update/delete**: generator meng-update status order & payment; verifikasi state terakhir benar di ClickHouse. *(update flow sudah dibuat; runtime verification menunggu Docker daemon aktif)*

**Deliverable demo:** dashboard query top product & payment success rate, plus bukti update status order tercermin di OLAP.

---

## MVP 3 — Reliability & Observability

**Tujuan:** bikin project terlihat production-minded.

- [ ] Docker Compose lengkap + **Kafka UI**.
- [ ] Dead-letter topic untuk event gagal.
- [ ] Dokumentasi konfigurasi Debezium connector.
- [ ] Monitoring lag sederhana (Kafka UI / system tables).
- [ ] Dashboard: **Grafana** atau **Metabase** terhubung ke ClickHouse.
- [ ] README lengkap dengan diagram arsitektur & contoh query demo.
- [ ] (Opsional) GIF/screenshot demo real-time order stream.

**Deliverable demo:** `docker compose up` → semuanya hidup, dashboard menampilkan metrik real-time, ada penanganan error.

---

## Ekstensi opsional (nilai tambah di interview)

- **Fraud-ish signal**: deteksi banyak payment gagal dari customer/IP yang sama dalam window tertentu (cocok untuk window function ClickHouse).
- **Customer Lifetime Value** sederhana: total revenue per customer.
- **Late / cancelled orders** analytics.
- **Schema evolution**: tunjukkan apa yang terjadi saat kolom baru ditambahkan di PostgreSQL.
- **Backfill / snapshot**: jelaskan cara Debezium melakukan initial snapshot (`op = 'r'`).
- **CI**: GitHub Actions untuk lint SQL / test generator.

---

## Estimasi urutan kerja

| Fase | Fokus | Output utama |
|------|-------|--------------|
| 1 | Infra + insert flow | pipeline jalan end-to-end |
| 2 | Model analitik + update/delete | fact/dim/agg + CDC penuh |
| 3 | Observability + dashboard | demo siap CV |

Mulai dari MVP 1. Jangan menambah komponen baru sebelum aliran data dasar terbukti jalan — ini menghindari debugging banyak hal sekaligus.
