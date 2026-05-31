package com.realtimeshop.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
class SeedDataLoader implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(SeedDataLoader.class);

    private final JdbcTemplate jdbc;

    SeedDataLoader(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void run(String... args) {
        seedCustomers();
        seedProducts();
        log.info("Seed data ready: {} customers, {} products",
                jdbc.queryForObject("SELECT count(*) FROM customers", Long.class),
                jdbc.queryForObject("SELECT count(*) FROM products", Long.class));
    }

    private void seedCustomers() {
        insertCustomer("maya@example.com", "Maya Santoso", "ID");
        insertCustomer("raka@example.com", "Raka Wijaya", "ID");
        insertCustomer("siti@example.com", "Siti Aminah", "MY");
        insertCustomer("daniel@example.com", "Daniel Tan", "SG");
        insertCustomer("amelia@example.com", "Amelia Putri", "ID");
    }

    private void insertCustomer(String email, String fullName, String country) {
        jdbc.update("""
                INSERT INTO customers (email, full_name, country)
                VALUES (?, ?, ?)
                ON CONFLICT (email) DO NOTHING
                """, email, fullName, country);
    }

    private void seedProducts() {
        insertProduct("SKU-KEYBOARD-001", "Mechanical Keyboard", "Accessories", "89.00", 120);
        insertProduct("SKU-MOUSE-001", "Wireless Mouse", "Accessories", "29.00", 200);
        insertProduct("SKU-MONITOR-001", "27 Inch Monitor", "Electronics", "239.00", 80);
        insertProduct("SKU-HEADSET-001", "Noise Cancelling Headset", "Audio", "119.00", 100);
        insertProduct("SKU-WEBCAM-001", "HD Webcam", "Electronics", "59.00", 140);
    }

    private void insertProduct(String sku, String name, String category, String price, int stockQty) {
        jdbc.update("""
                INSERT INTO products (sku, name, category, price, stock_qty)
                VALUES (?, ?, ?, ?::numeric, ?)
                ON CONFLICT (sku) DO NOTHING
                """, sku, name, category, price, stockQty);
    }
}
