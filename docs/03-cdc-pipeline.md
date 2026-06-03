# 03 — CDC Pipeline

Dokumen ini menjelaskan cara kerja pipeline CDC dari PostgreSQL → Debezium → Kafka → ClickHouse, lengkap dengan contoh konfigurasi.

---

## 1. PostgreSQL: aktifkan logical replication

Tambahkan ke `postgresql.conf`:

```conf
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
```

Buat user replikasi:

```sql
CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
GRANT CONNECT ON DATABASE shop TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;

-- publication untuk pgoutput
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
```

---

## 2. Debezium connector

`debezium/postgres-connector.json`:

```json
{
  "name": "shop-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "debezium",
    "database.dbname": "shop",
    "topic.prefix": "shop",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "disabled",
    "slot.name": "dbz_slot",
    "table.include.list": "public.customers,public.products,public.orders,public.order_items,public.payments,public.inventory_events",
    "decimal.handling.mode": "string",
    "time.precision.mode": "connect",
    "tombstones.on.delete": "false",
    "snapshot.mode": "initial",
    "errors.tolerance": "all",
    "errors.retry.timeout": "60000",
    "errors.retry.delay.max.ms": "5000",
    "errors.deadletterqueue.topic.name": "shop.dlq",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true"
  }
}
```

Topic yang dihasilkan (format `<topic.prefix>.<schema>.<table>`):

```
shop.public.customers
shop.public.products
shop.public.orders
shop.public.order_items
shop.public.payments
shop.public.inventory_events
```

> Catatan: model konseptual masih menyebut `shipments`, tetapi connector MVP saat ini hanya mereplikasi tabel yang sudah punya alur OLAP/materialized view aktif.

---

## 3. Struktur event Debezium

Setiap pesan Kafka berisi `payload` dengan bentuk:

```json
{
  "payload": {
    "before": { ... },        // state lama (null saat insert)
    "after":  { ... },        // state baru (null saat delete)
    "op": "c",                // c=create, u=update, d=delete, r=read snapshot
    "ts_ms": 1716900000000,
    "source": {
      "lsn": 123456789,
      "table": "orders",
      "txId": 555
    }
  }
}
```

Kolom kunci untuk OLAP:

- `op` → menentukan insert/update/delete.
- `source.lsn` atau `ts_ms` → dipakai sebagai `_version` untuk dedup di `ReplacingMergeTree`.

---

## 4. ClickHouse: Kafka Engine table (raw)

Tabel ini membaca JSON mentah dari topic. Pakai satu kolom `message String` lalu parse di Materialized View (paling fleksibel), atau map langsung field Debezium.

Contoh untuk `orders`:

```sql
CREATE TABLE kafka_orders_raw
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
```

---

## 5. ClickHouse: Materialized View (transform → fact)

MV membaca dari `kafka_orders_raw`, mem-parse JSON Debezium, dan menulis ke `fact_orders`.

```sql
CREATE MATERIALIZED VIEW mv_orders TO fact_orders AS
SELECT
    JSONExtractUInt(message, 'payload', 'after', 'id')            AS order_id,
    JSONExtractUInt(message, 'payload', 'after', 'customer_id')   AS customer_id,
    JSONExtractString(message, 'payload', 'after', 'status')      AS status,
    toDecimal64(JSONExtractString(message, 'payload', 'after', 'total_amount'), 2) AS total_amount,
    fromUnixTimestamp64Milli(JSONExtractUInt(message, 'payload', 'after', 'created_at')) AS created_at,
    fromUnixTimestamp64Milli(JSONExtractUInt(message, 'payload', 'ts_ms'))              AS updated_at,
    JSONExtractUInt(message, 'payload', 'source', 'lsn')          AS _version,
    if(JSONExtractString(message, 'payload', 'op') = 'd', 1, 0)   AS is_deleted
FROM kafka_orders_raw
WHERE JSONExtractString(message, 'payload', 'op') IN ('c', 'u', 'd', 'r');
```

> Untuk event delete (`op = 'd'`), field `after` bernilai null sehingga pakai `before` untuk mengambil `id`. Pada implementasi nyata, tambahkan logika `if(op='d', before.id, after.id)`.

### Mengambil state terbaru di query

Karena `fact_orders` memakai `ReplacingMergeTree(_version)`, gunakan `FINAL` atau `argMax` saat query agar mendapat versi terakhir:

```sql
SELECT order_id, argMax(status, _version) AS status
FROM fact_orders
GROUP BY order_id
HAVING argMax(is_deleted, _version) = 0;
```

---

## 6. Materialized View untuk agregasi

Contoh `agg_revenue_per_minute` dari `fact_orders` (hanya order berbayar):

```sql
CREATE MATERIALIZED VIEW mv_revenue_per_minute TO agg_revenue_per_minute AS
SELECT
    toStartOfMinute(created_at) AS minute,
    count()                     AS order_count,
    sum(total_amount)           AS revenue
FROM fact_orders
WHERE status = 'paid'
GROUP BY minute;
```

Contoh `agg_payment_status_hourly`:

```sql
CREATE MATERIALIZED VIEW mv_payment_status_hourly TO agg_payment_status_hourly AS
SELECT
    toStartOfHour(created_at) AS hour,
    status,
    count()                   AS payment_count,
    sum(amount)               AS total_amount
FROM fact_payments
GROUP BY hour, status;
```

---

## 7. Penanganan update & delete (ringkas)

| Event      | OLTP                | Penanganan di ClickHouse                                                                  |
| ---------- | ------------------- | ----------------------------------------------------------------------------------------- |
| `c` insert | INSERT order        | tulis baris baru ke fact, `_version` = lsn                                                |
| `u` update | UPDATE status order | tulis baris baru dengan `_version` lebih tinggi → `ReplacingMergeTree` ambil yang terbaru |
| `d` delete | DELETE order        | tulis baris `is_deleted = 1` dengan `_version` tertinggi                                  |
| `r` read   | snapshot awal       | diperlakukan seperti insert                                                               |

---

## 8. Reliability & Observability

### Dead-letter topic

Konfigurasi error handling di Kafka Connect:

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

### Memantau consumer lag

```sql
-- ClickHouse: status consumer Kafka
SELECT * FROM system.kafka_consumers;
```

Gunakan **Kafka UI** (http://localhost:8080) untuk memantau lag per consumer group dan inspeksi pesan.

### Memantau ingest ClickHouse

```sql
SELECT table, sum(rows) AS rows, count() AS parts
FROM system.parts
WHERE active
GROUP BY table;
```

---

## Urutan menjalankan

1. `docker compose up -d` (postgres, kafka, connect, clickhouse, kafka-ui).
2. Pastikan PostgreSQL siap dan publication dibuat.
3. POST connector config ke Kafka Connect (`:8083/connectors`).
4. Buat tabel ClickHouse (raw, fact, dim, agg) + Materialized Views.
5. Jalankan Spring Boot generator.
6. Verifikasi data mengalir: cek topic di Kafka UI → cek `fact_orders` → cek `agg_revenue_per_minute`.
