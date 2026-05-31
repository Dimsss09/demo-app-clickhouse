# 01 — Architecture

## Tujuan

Membangun pipeline **OLTP → OLAP** yang mensinkronkan data transaksi e-commerce dari PostgreSQL ke ClickHouse secara *near real-time* menggunakan Change Data Capture (CDC), sehingga query analitik (revenue, produk terlaris, payment rate, stok) bisa dilakukan dengan latensi rendah tanpa membebani database transaksional.

## Diagram Alir Data

```
┌─────────────────────────┐
│ Spring Boot Generator/API│  Menghasilkan transaksi: order, payment, stok
└────────────┬────────────┘
             │ INSERT / UPDATE / DELETE
             ▼
┌─────────────────────────┐
│   PostgreSQL (OLTP)      │  wal_level = logical
│  customers, products,    │
│  orders, order_items,    │
│  payments, shipments,    │
│  inventory_events        │
└────────────┬────────────┘
             │ logical decoding (pgoutput)
             ▼
┌─────────────────────────┐
│ Debezium (Kafka Connect) │  Menangkap perubahan WAL → event c/u/d/r
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│      Kafka Topics        │  satu topic per tabel
└────────────┬────────────┘
             │ consume
             ▼
┌─────────────────────────┐
│ ClickHouse Kafka Engine  │  tabel "raw" pembaca topic
└────────────┬────────────┘
             │ Materialized View (transform)
             ▼
┌─────────────────────────┐
│  ClickHouse OLAP Tables  │  fact / dim / agg
└────────────┬────────────┘
             │ SQL
             ▼
┌─────────────────────────┐
│  Dashboard / Query API   │  Grafana / Metabase / REST
└─────────────────────────┘
```

## Komponen

### 1. Spring Boot Data Generator / API
Mensimulasikan aplikasi e-commerce nyata. Bertugas:
- Membuat customer & product (data master).
- Membuat order dan order_items.
- Mensimulasikan pembayaran (sukses / gagal).
- Mengubah status order (`u` event) dan pergerakan stok.

Tujuannya menghasilkan aliran event yang realistis termasuk **update** dan **delete**, bukan hanya insert.

### 2. PostgreSQL (OLTP)
Sumber kebenaran (source of truth) untuk data transaksi. Dikonfigurasi dengan `wal_level = logical` agar Debezium dapat membaca Write-Ahead Log.

### 3. Debezium (Kafka Connect)
Membaca logical replication slot PostgreSQL dan mengubah setiap perubahan baris menjadi event Kafka. Tiap event punya field `op`:
- `c` — create (insert)
- `u` — update
- `d` — delete
- `r` — read (snapshot awal)

### 4. Apache Kafka
Event backbone. Satu topic per tabel sumber. Memberi *decoupling* antara sumber dan tujuan, serta buffer saat ClickHouse lambat.

### 5. ClickHouse
- **Kafka Engine tables** — membaca event mentah dari topic.
- **Materialized Views** — mentransform event mentah, parse payload Debezium, dan menulis ke tabel OLAP final.
- **OLAP tables** — fact, dimension, dan agregasi yang dioptimalkan untuk analitik.

### 6. Observability
- **Kafka UI** untuk inspeksi topic & consumer lag.
- **Dead-letter topic** untuk event yang gagal diproses.
- **ClickHouse system tables** (`system.kafka_consumers`, `system.parts`) untuk memantau ingest.

## Keputusan Desain

| Keputusan | Alasan |
|-----------|--------|
| CDC via Debezium, bukan batch ETL | Latensi rendah, tidak membebani OLTP dengan query polling, menangkap setiap perubahan termasuk delete. |
| Kafka di tengah | Decoupling source & sink, tahan lonjakan beban, bisa multi-consumer. |
| ClickHouse Kafka Engine + MV | Pola standar ClickHouse untuk ingest streaming; transform terjadi di dalam ClickHouse. |
| `ReplacingMergeTree` untuk dimension | Menangani update data master (customer/product) dengan menyimpan versi terbaru. |
| `MergeTree` untuk fact | Append-only, cocok untuk event transaksi yang sudah terjadi. |
| Materialized View untuk agregasi | Pre-aggregate (SummingMergeTree / AggregatingMergeTree) agar query dashboard cepat. |

## Penanganan Update & Delete (poin kunci)

Banyak project CDC hanya menangani insert. Project ini secara eksplisit menangani:
- **Update status order** (`pending → paid → shipped → delivered/cancelled`).
- **Update status payment** (`pending → success/failed`).
- **Delete** baris (soft handling via flag `is_deleted` di ClickHouse).

Di ClickHouse, event `u`/`d` ditangani dengan menyimpan kolom versi (`_version` dari LSN/timestamp) dan menggunakan `ReplacingMergeTree(_version)` atau kolom `sign` pada pola collapsing, sehingga state terakhir selalu bisa direkonstruksi.

Lihat detail implementasi di [03 — CDC Pipeline](./03-cdc-pipeline.md).
