package com.realtimeshop.generator;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
class OrderGenerator {

    private static final Logger log = LoggerFactory.getLogger(OrderGenerator.class);
    private static final List<String> PAYMENT_METHODS = List.of("card", "bank_transfer", "ewallet");
    private static final List<String> NEXT_SHIPPING_STATUSES = List.of("shipped", "delivered");

    private final JdbcTemplate jdbc;

    OrderGenerator(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Scheduled(
            initialDelayString = "${generator.initial-delay-ms}",
            fixedRateString = "${generator.fixed-rate-ms}"
    )
    @Transactional
    public void generatePendingOrder() {
        Customer customer = pickCustomer();
        Product product = pickProductWithStock();
        int quantity = ThreadLocalRandom.current().nextInt(1, Math.min(product.stockQty(), 3) + 1);
        BigDecimal totalAmount = product.price().multiply(BigDecimal.valueOf(quantity));

        Long orderId = jdbc.queryForObject("""
                INSERT INTO orders (customer_id, status, total_amount)
                VALUES (?, 'pending', ?)
                RETURNING id
                """, Long.class, customer.id(), totalAmount);

        jdbc.update("""
                INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                VALUES (?, ?, ?, ?)
                """, orderId, product.id(), quantity, product.price());

        Long paymentId = jdbc.queryForObject("""
                INSERT INTO payments (order_id, method, status, amount)
                VALUES (?, ?, 'pending', ?)
                RETURNING id
                """, Long.class, orderId, randomPaymentMethod(), totalAmount);

        log.info("Created pending order_id={} payment_id={} customer={} product={} qty={} total={}",
                orderId, paymentId, customer.email(), product.sku(), quantity, totalAmount);
    }

    @Scheduled(
            initialDelayString = "${generator.initial-delay-ms}",
            fixedRateString = "${generator.status-update-rate-ms}"
    )
    @Transactional
    public void advanceOrdersAndPayments() {
        List<PendingPayment> pendingPayments = jdbc.query("""
                SELECT p.id AS payment_id, p.order_id, p.amount
                FROM payments p
                WHERE p.status = 'pending'
                ORDER BY p.created_at
                LIMIT 5
                """, (rs, rowNum) -> new PendingPayment(
                rs.getLong("payment_id"),
                rs.getLong("order_id"),
                rs.getBigDecimal("amount")));

        for (PendingPayment payment : pendingPayments) {
            boolean success = ThreadLocalRandom.current().nextInt(100) < 85;
            if (success) {
                markPaymentSuccess(payment);
            } else {
                markPaymentFailed(payment);
            }
        }

        List<Long> paidOrders = jdbc.queryForList("""
                SELECT id
                FROM orders
                WHERE status = 'paid'
                ORDER BY updated_at
                LIMIT 3
                """, Long.class);

        for (Long orderId : paidOrders) {
            String nextStatus = NEXT_SHIPPING_STATUSES.get(ThreadLocalRandom.current().nextInt(NEXT_SHIPPING_STATUSES.size()));
            jdbc.update("UPDATE orders SET status = ? WHERE id = ?", nextStatus, orderId);
            log.info("Advanced order_id={} to status={}", orderId, nextStatus);
        }
    }

    @Scheduled(
            initialDelayString = "${generator.restock-initial-delay-ms}",
            fixedRateString = "${generator.restock-rate-ms}"
    )
    @Transactional
    public void restockRandomProduct() {
        Product product = pickAnyProduct();
        int quantity = ThreadLocalRandom.current().nextInt(5, 21);

        jdbc.update("""
                UPDATE products
                SET stock_qty = stock_qty + ?
                WHERE id = ?
                """, quantity, product.id());

        jdbc.update("""
                INSERT INTO inventory_events (product_id, change_qty, reason)
                VALUES (?, ?, 'restock')
                """, product.id(), quantity);

        log.info("Restocked product={} qty={}", product.sku(), quantity);
    }

    @Scheduled(
            initialDelayString = "${generator.delete-initial-delay-ms}",
            fixedRateString = "${generator.delete-rate-ms}"
    )
    @Transactional
    public void deleteSomeCancelledOrders() {
        List<Long> cancelledOrderIds = jdbc.queryForList("""
                SELECT id
                FROM orders
                WHERE status = 'cancelled'
                  AND updated_at < now() - interval '10 seconds'
                ORDER BY updated_at
                LIMIT 2
                """, Long.class);

        for (Long orderId : cancelledOrderIds) {
            jdbc.update("DELETE FROM orders WHERE id = ?", orderId);
            log.info("Deleted cancelled order_id={} to produce CDC delete events", orderId);
        }
    }

    private void markPaymentSuccess(PendingPayment payment) {
        jdbc.update("UPDATE payments SET status = 'success' WHERE id = ?", payment.paymentId());
        jdbc.update("UPDATE orders SET status = 'paid' WHERE id = ?", payment.orderId());

        List<OrderItem> items = jdbc.query("""
                SELECT product_id, quantity
                FROM order_items
                WHERE order_id = ?
                """, (rs, rowNum) -> new OrderItem(
                rs.getLong("product_id"),
                rs.getInt("quantity")), payment.orderId());

        for (OrderItem item : items) {
            jdbc.update("""
                    UPDATE products
                    SET stock_qty = GREATEST(stock_qty - ?, 0)
                    WHERE id = ?
                    """, item.quantity(), item.productId());

            jdbc.update("""
                    INSERT INTO inventory_events (product_id, change_qty, reason)
                    VALUES (?, ?, 'sale')
                    """, item.productId(), -item.quantity());
        }

        log.info("Payment success payment_id={} order_id={} amount={}",
                payment.paymentId(), payment.orderId(), payment.amount());
    }

    private void markPaymentFailed(PendingPayment payment) {
        jdbc.update("UPDATE payments SET status = 'failed' WHERE id = ?", payment.paymentId());
        jdbc.update("UPDATE orders SET status = 'cancelled' WHERE id = ?", payment.orderId());

        log.info("Payment failed payment_id={} order_id={}", payment.paymentId(), payment.orderId());
    }

    private Customer pickCustomer() {
        List<Customer> customers = jdbc.query("""
                SELECT id, email
                FROM customers
                ORDER BY random()
                LIMIT 1
                """, (rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("email")));

        if (customers.isEmpty()) {
            throw new IllegalStateException("No customers available. Seed data did not run.");
        }
        return customers.getFirst();
    }

    private Product pickProductWithStock() {
        List<Product> products = jdbc.query("""
                SELECT id, sku, price, stock_qty
                FROM products
                WHERE stock_qty > 0
                ORDER BY random()
                LIMIT 1
                """, (rs, rowNum) -> new Product(
                rs.getLong("id"),
                rs.getString("sku"),
                rs.getBigDecimal("price"),
                rs.getInt("stock_qty")));

        if (products.isEmpty()) {
            throw new IllegalStateException("No products with stock available. Wait for restock events.");
        }
        return products.getFirst();
    }

    private Product pickAnyProduct() {
        List<Product> products = jdbc.query("""
                SELECT id, sku, price, stock_qty
                FROM products
                ORDER BY random()
                LIMIT 1
                """, (rs, rowNum) -> new Product(
                rs.getLong("id"),
                rs.getString("sku"),
                rs.getBigDecimal("price"),
                rs.getInt("stock_qty")));

        if (products.isEmpty()) {
            throw new IllegalStateException("No products available. Seed data did not run.");
        }
        return products.getFirst();
    }

    private String randomPaymentMethod() {
        return PAYMENT_METHODS.get(ThreadLocalRandom.current().nextInt(PAYMENT_METHODS.size()));
    }

    private record Customer(Long id, String email) {
    }

    private record Product(Long id, String sku, BigDecimal price, int stockQty) {
    }

    private record PendingPayment(Long paymentId, Long orderId, BigDecimal amount) {
    }

    private record OrderItem(Long productId, int quantity) {
    }
}
