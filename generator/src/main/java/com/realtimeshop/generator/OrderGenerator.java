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

    private final JdbcTemplate jdbc;

    OrderGenerator(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Scheduled(
            initialDelayString = "${generator.initial-delay-ms}",
            fixedRateString = "${generator.fixed-rate-ms}"
    )
    @Transactional
    public void generatePaidOrder() {
        Customer customer = pickCustomer();
        Product product = pickProduct();
        int quantity = ThreadLocalRandom.current().nextInt(1, 4);
        BigDecimal totalAmount = product.price().multiply(BigDecimal.valueOf(quantity));

        Long orderId = jdbc.queryForObject("""
                INSERT INTO orders (customer_id, status, total_amount)
                VALUES (?, 'paid', ?)
                RETURNING id
                """, Long.class, customer.id(), totalAmount);

        jdbc.update("""
                INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                VALUES (?, ?, ?, ?)
                """, orderId, product.id(), quantity, product.price());

        jdbc.update("""
                INSERT INTO payments (order_id, method, status, amount)
                VALUES (?, ?, 'success', ?)
                """, orderId, randomPaymentMethod(), totalAmount);

        log.info("Created order_id={} customer={} product={} qty={} total={}",
                orderId, customer.email(), product.sku(), quantity, totalAmount);
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

    private Product pickProduct() {
        List<Product> products = jdbc.query("""
                SELECT id, sku, price
                FROM products
                ORDER BY random()
                LIMIT 1
                """, (rs, rowNum) -> new Product(
                rs.getLong("id"),
                rs.getString("sku"),
                rs.getBigDecimal("price")));

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

    private record Product(Long id, String sku, BigDecimal price) {
    }
}
