# 02 — Data Model

Dokumen ini mendefinisikan skema **OLTP (PostgreSQL)** sebagai sumber dan skema **OLAP (ClickHouse)** sebagai tujuan analitik.

---

## A. Skema OLTP — PostgreSQL

Database transaksional yang dinormalisasi (3NF). Ini adalah *source of truth*.

```
customers ───< orders ───< order_items >─── products
                  │
                  ├──< payments
                  └──< shipments
products ───< inventory_events
```

### customers
| Kolom | Tipe | Keterangan |
|-------|------|-----------|
| id | BIGSERIAL PK | |
| email | TEXT UNIQUE | |
| full_name | TEXT | |
| country | TEXT | |
| created_at | TIMESTAMPTZ | |
| updated_at | TIMESTAMPTZ | untuk melacak update |

### products
| Kolom | Tipe | Keterangan |
|-------|------|-----------|
| id | BIGSERIAL PK | |
| sku | TEXT UNIQUE | |
| name | TEXT | |
| category | TEXT | |
| price | NUMERIC(12,2) | harga jual saat ini |
| stock_qty | INTEGER | stok berjalan |
| created_at | TIMESTAMPTZ | |
| updated_at | TIMESTAMPTZ | |

### orders
| Kolom | Tipe | Keterangan |
|-------|------|-----------|
| id | BIGSERIAL PK | |
| customer_id | BIGINT FK → customers.id | |
| status | TEXT | `pending`, `paid`, `shipped`, `delivered`, `cancelled` |
| total_amount | NUMERIC(12,2) | |
| created_at | TIMESTAMPTZ | |
| updated_at | TIMESTAMPTZ | berubah saat status berubah |

### order_items
| Kolom | Tipe | Keterangan |
|-------|------|-----------|
| id | BIGSERIAL PK | |
| order_id | BIGINT FK → orders.id | |
| product_id | BIGINT FK → products.id | |
| quantity | INTEGER | |
| unit_price | NUMERIC(12,2) | harga saat transaksi (snapshot) |

### payments
| Kolom | Tipe | Keterangan |
|-------|------|-----------|
| id | BIGSERIAL PK | |
| order_id | BIGINT FK → orders.id | |
| method | TEXT | `card`, `bank_transfer`, `ewallet` |
| status | TEXT | `pending`, `success`, `failed` |
| amount | NUMERIC(12,2) | |
| created_at | TIMESTAMPTZ | |
| updated_at | TIMESTAMPTZ | |

### shipments
| Kolom | Tipe | Keterangan |
|-------|------|-----------|
| id | BIGSERIAL PK | |
| order_id | BIGINT FK → orders.id | |
| status | TEXT | `preparing`, `in_transit`, `delivered` |
| shipped_at | TIMESTAMPTZ | |
| delivered_at | TIMESTAMPTZ | |

### inventory_events
Tabel append-only yang mencatat setiap pergerakan stok (bagus untuk OLAP).
| Kolom | Tipe | Keterangan |
|-------|------|-----------|
| id | BIGSERIAL PK | |
| product_id | BIGINT FK → products.id | |
| change_qty | INTEGER | + restock, − penjualan |
| reason | TEXT | `sale`, `restock`, `adjustment` |
| created_at | TIMESTAMPTZ | |

---

## B. Skema OLAP — ClickHouse

Didenormalisasi dan dioptimalkan untuk query analitik. Terbagi menjadi tiga lapis: **raw (Kafka Engine)** → **fact/dim** → **agg**.

### Prinsip pemilihan engine
- **Fact tables** → `MergeTree` (append-only, partisi per hari).
- **Dimension tables** → `ReplacingMergeTree(_version)` (ambil versi terbaru saat ada update).
- **Aggregation tables** → `SummingMergeTree` / `AggregatingMergeTree` diisi oleh Materialized View.

### Dimension tables

#### dim_customers
```sql
CREATE TABLE dim_customers
(
    id          UInt64,
    email       String,
    full_name   String,
    country     String,
    created_at  DateTime64(3),
    updated_at  DateTime64(3),
    _version    UInt64,          -- dari LSN / updated_at untuk dedup
    is_deleted  UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY id;
```

#### dim_products
```sql
CREATE TABLE dim_products
(
    id          UInt64,
    sku         String,
    name        String,
    category    String,
    price       Decimal(12,2),
    stock_qty   Int32,
    updated_at  DateTime64(3),
    _version    UInt64,
    is_deleted  UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY id;
```

### Fact tables

#### fact_orders
```sql
CREATE TABLE fact_orders
(
    order_id     UInt64,
    customer_id  UInt64,
    status       LowCardinality(String),
    total_amount Decimal(12,2),
    created_at   DateTime64(3),
    updated_at   DateTime64(3),
    _version     UInt64,
    is_deleted   UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)   -- status order bisa di-update
PARTITION BY toYYYYMM(created_at)
ORDER BY (order_id);
```

#### fact_payments
```sql
CREATE TABLE fact_payments
(
    payment_id  UInt64,
    order_id    UInt64,
    method      LowCardinality(String),
    status      LowCardinality(String),
    amount      Decimal(12,2),
    created_at  DateTime64(3),
    updated_at  DateTime64(3),
    _version    UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (payment_id);
```

#### fact_inventory_events
```sql
CREATE TABLE fact_inventory_events
(
    event_id    UInt64,
    product_id  UInt64,
    change_qty  Int32,
    reason      LowCardinality(String),
    created_at  DateTime64(3)
)
ENGINE = MergeTree                      -- append-only, tidak ada update
PARTITION BY toYYYYMM(created_at)
ORDER BY (product_id, created_at);
```

### Aggregation tables (diisi via Materialized View)

#### agg_revenue_per_minute
```sql
CREATE TABLE agg_revenue_per_minute
(
    minute        DateTime,
    order_count   UInt64,
    revenue       Decimal(18,2)
)
ENGINE = SummingMergeTree
ORDER BY (minute);
```

#### agg_product_sales_daily
```sql
CREATE TABLE agg_product_sales_daily
(
    day            Date,
    product_id     UInt64,
    units_sold     UInt64,
    revenue        Decimal(18,2)
)
ENGINE = SummingMergeTree
ORDER BY (day, product_id);
```

#### agg_payment_status_hourly
```sql
CREATE TABLE agg_payment_status_hourly
(
    hour           DateTime,
    status         LowCardinality(String),
    payment_count  UInt64,
    total_amount   Decimal(18,2)
)
ENGINE = SummingMergeTree
ORDER BY (hour, status);
```

---

## Pemetaan OLTP → OLAP

| Sumber (PostgreSQL) | Tujuan (ClickHouse) | Catatan |
|---------------------|---------------------|---------|
| customers | dim_customers | update via ReplacingMergeTree |
| products | dim_products | update via ReplacingMergeTree |
| orders | fact_orders | status order di-update |
| order_items | (feed) agg_product_sales_daily | digabung dengan products untuk revenue |
| payments | fact_payments + agg_payment_status_hourly | |
| inventory_events | fact_inventory_events | append-only |
| orders + order_items | agg_revenue_per_minute | revenue real-time |

Detail Materialized View untuk setiap transformasi ada di [03 — CDC Pipeline](./03-cdc-pipeline.md).
