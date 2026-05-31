package com.realtimeshop.generator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class RealtimeShopGeneratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(RealtimeShopGeneratorApplication.class, args);
    }
}
