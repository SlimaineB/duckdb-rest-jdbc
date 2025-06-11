package com.slim.config;

import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;

public class DuckDBConnectionCustomizer {

    public static void initialize(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             BufferedReader reader = new BufferedReader(new InputStreamReader(
                     DuckDBConnectionCustomizer.class.getResourceAsStream("/init.sql")))) {

            StringBuilder sql = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sql.append(line).append("\n");
            }

            // Exécuter le script SQL
            statement.execute(sql.toString());
            System.out.println("Initialization script executed successfully.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute initialization script", e);
        }
    }
}