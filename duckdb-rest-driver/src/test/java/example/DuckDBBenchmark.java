package example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DuckDBBenchmark {

    private static final String JDBC_URL = "jdbc:duckdb://localhost:8080?useEncryption=false&disableCertificateVerification=true"; // Remplace par ton URL
    private static final int THREAD_COUNT = 10; // Nombre de threads pour le benchmark
    private static final int QUERY_COUNT = 1000; // Nombre total de requêtes à exécuter

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int i = 0; i < QUERY_COUNT; i++) {
            executor.submit(() -> {
                long startTime = System.currentTimeMillis();
                try (Connection conn = DriverManager.getConnection(JDBC_URL);
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT count(1) FROM read_parquet('s3://test-bucket/data/init/**/*.parquet')")) {

                    while (rs.next()) {
                        // Lecture des résultats (facultatif)
                        int count = rs.getInt(1);
                        System.out.println("Résultat: " + count);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
                long endTime = System.currentTimeMillis();
                System.out.println("Requête exécutée en " + (endTime - startTime) + " ms");
            });
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                System.err.println("Le benchmark a pris trop de temps !");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Benchmark terminé !");
    }
}