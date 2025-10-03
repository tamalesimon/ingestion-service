package com.tamalesp.ingestionservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;

@SpringBootApplication
@EnableR2dbcRepositories(basePackages = "com.tamalesp.ingestionservice.repository")
public class IngestionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestionServiceApplication.class, args);
    }

}
