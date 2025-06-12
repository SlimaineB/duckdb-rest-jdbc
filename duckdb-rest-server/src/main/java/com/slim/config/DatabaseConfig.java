package com.slim.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.stream.Collectors;

@Configuration
public class DatabaseConfig {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfig.class);

    // Injection de la propriété depuis application.yml (facultatif)
    @Value("${app.init-sql-path:}")
    private String initSqlPath;

    @Bean
    @ConfigurationProperties("spring.datasource.hikari")
    public HikariConfig hikariConfig() {
        return new HikariConfig();
    }

    @Bean
    public DataSource hikariDataSource(HikariConfig hikariConfig, DataSourceProperties dataSourceProperties) {
        hikariConfig.setJdbcUrl(dataSourceProperties.getUrl());
        hikariConfig.setDriverClassName(dataSourceProperties.getDriverClassName());
        hikariConfig.setUsername(dataSourceProperties.getUsername());
        hikariConfig.setPassword(dataSourceProperties.getPassword());

        logger.info("HikariConfig - JDBC URL: {}", hikariConfig.getJdbcUrl());
        logger.info("HikariConfig - Driver Class Name: {}", hikariConfig.getDriverClassName());
        logger.info("HikariConfig - Username: {}", hikariConfig.getUsername());
        logger.info("HikariConfig - Maximum Pool Size: {}", hikariConfig.getMaximumPoolSize());
        logger.info("HikariConfig - Connection Timeout: {}", hikariConfig.getConnectionTimeout());

        // Charger le script SQL d'initialisation SI le chemin est fourni
        if (initSqlPath != null && !initSqlPath.trim().isEmpty()) {
            File initFile = new File(initSqlPath);
            if (!initFile.exists() || !initFile.isFile()) {
                logger.error("Initialization script not found at path: {}", initSqlPath);
                throw new RuntimeException("Initialization script not found at path: " + initSqlPath);
            }
            try (BufferedReader reader = new BufferedReader(new FileReader(initFile))) {
                String initSql = reader.lines().collect(Collectors.joining("\n"));
                logger.info("Initialization script loaded successfully from {}:\n{}", initSqlPath, initSql);
                hikariConfig.setConnectionInitSql(initSql);
            } catch (Exception e) {
                logger.error("Failed to load initialization script from {}", initSqlPath, e);
                throw new RuntimeException("Failed to load initialization script", e);
            }
        } else {
            logger.info("No initialization script configured (app.init-sql-path is empty or missing).");
        }

        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        logger.info("HikariDataSource created successfully.");
        return dataSource;
    }
}