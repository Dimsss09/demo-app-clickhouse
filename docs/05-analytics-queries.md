# 05 — Analytics Queries

Kumpulan query analitik ClickHouse untuk demo. Cocok ditunjukkan saat interview karena memperlihatkan nilai bisnis dari pipeline.

> Catatan: tabel `fact_*` memakai `ReplacingMergeTree`, jadi untuk state terkini gunakan `argMax(col, _version)` atau `... FINAL`.

---

## 1. Revenue real-time (per menit)

```sql
SELECT
    minute,
    order_count,
    revenue
FROM agg_revenue_per_minute
ORDER BY minute DESC
LIMIT 15;
```

## 2. Revenue hari ini (running total)

```sql
SELECT
    sum(revenue)     AS revenue_today,
    sum(order_count) AS orders_today
FROM agg_revenue_per_minute
WHERE toDate(minute) = today();
```

## 3. Top 10 produk terlaris (hari ini)

```sql
SELECT
    p.name        AS product,
    p.category    AS category,
    sum(s.units_sold) AS units_sold,
    sum(s.revenue)    AS revenue
FROM agg_product_sales_daily AS s
INNER JOIN dim_products AS p ON p.id = s.product_id
WHERE s.day = today()
GROUP BY product, category
ORDER BY units_sold DESC
LIMIT 10;
```

## 4. Payment success rate (per jam)

```sql
SELECT
    hour,
    sumIf(payment_count, status = 'success') AS success,
    sumIf(payment_count, status = 'failed')  AS failed,
    round(success / nullIf(success + failed, 0) * 100, 2) AS success_rate_pct
FROM agg_payment_status_hourly
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;
```

## 5. Conversion order → paid

```sql
SELECT
    countIf(status = 'paid')    AS paid_orders,
    count()                     AS total_orders,
    round(countIf(status = 'paid') / count() * 100, 2) AS conversion_pct
FROM
(
    SELECT order_id, argMax(status, _version) AS status
    FROM fact_orders
    GROUP BY order_id
    HAVING argMax(is_deleted, _version) = 0
);
```

## 6. Customer Lifetime Value (CLV) sederhana

```sql
SELECT
    c.full_name AS customer,
    c.country   AS country,
    sum(o.total_amount) AS lifetime_value,
    count()             AS total_orders
FROM
(
    SELECT order_id, argMax(customer_id, _version) AS customer_id,
           argMax(total_amount, _version) AS total_amount,
           argMax(status, _version) AS status
    FROM fact_orders
    GROUP BY order_id
    HAVING argMax(is_deleted, _version) = 0
) AS o
INNER JOIN dim_customers AS c ON c.id = o.customer_id
WHERE o.status IN ('paid', 'shipped', 'delivered')
GROUP BY customer, country
ORDER BY lifetime_value DESC
LIMIT 20;
```

## 7. Inventory movement (pergerakan stok)

```sql
SELECT
    p.name AS product,
    sumIf(change_qty, reason = 'sale')     AS sold,
    sumIf(change_qty, reason = 'restock')  AS restocked,
    sum(change_qty)                        AS net_change
FROM fact_inventory_events AS e
INNER JOIN dim_products AS p ON p.id = e.product_id
WHERE toDate(created_at) = today()
GROUP BY product
ORDER BY sold ASC
LIMIT 20;
```

## 8. Live order stream (50 order terbaru)

```sql
SELECT
    order_id,
    argMax(status, _version)       AS status,
    argMax(total_amount, _version) AS amount,
    max(created_at)                AS created_at
FROM fact_orders
GROUP BY order_id
ORDER BY created_at DESC
LIMIT 50;
```

## 9. Cancelled / late orders

```sql
SELECT
    toDate(created_at) AS day,
    countIf(status = 'cancelled') AS cancelled,
    count()                       AS total,
    round(countIf(status = 'cancelled') / count() * 100, 2) AS cancel_rate_pct
FROM
(
    SELECT order_id,
           argMax(status, _version) AS status,
           min(created_at) AS created_at
    FROM fact_orders
    GROUP BY order_id
)
GROUP BY day
ORDER BY day DESC;
```

## 10. Fraud-ish signal — banyak payment gagal dari customer yang sama

```sql
SELECT
    o.customer_id,
    count()                         AS failed_payments,
    min(p.created_at)               AS first_failed,
    max(p.created_at)               AS last_failed
FROM fact_payments AS p
INNER JOIN
(
    SELECT order_id, argMax(customer_id, _version) AS customer_id
    FROM fact_orders GROUP BY order_id
) AS o ON o.order_id = p.order_id
WHERE p.status = 'failed'
GROUP BY o.customer_id
HAVING failed_payments >= 5
   AND dateDiff('minute', first_failed, last_failed) <= 30
ORDER BY failed_payments DESC;
```

---

Query-query ini bisa langsung dipasang sebagai panel di **Grafana** atau **Metabase** untuk dashboard demo.
