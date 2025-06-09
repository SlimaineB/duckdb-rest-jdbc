package com.slim.driver;



import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class DuckMetaTest {
    public static void main(String[] args) {
        try {
            // Chargement du driver REST personnalisé
            Class.forName("com.slim.duckdb.DuckDBDriver");

            // Connexion au serveur REST
            String url = "jdbc:duckdb://localhost:8080/";
            try (Connection conn = DriverManager.getConnection(url)) {

                DatabaseMetaData meta = conn.getMetaData();
 
                // 1. Infos générales
                System.out.println("Driver: " + meta.getDriverName() + " v" + meta.getDriverVersion());
                System.out.println("DB: " + meta.getDatabaseProductName() + " v" + meta.getDatabaseProductVersion());

                // 2. Liste des schémas
                System.out.println("\n=== Schemas ===");
                try (ResultSet rs = meta.getSchemas()) {
                    while (rs.next()) {
                        System.out.println(" - " + rs.getString("TABLE_SCHEM"));
                    }
                }

                // 3. Liste des tables
                System.out.println("\n=== Tables ===");
                try (ResultSet rs = meta.getTables("memory", "main", "%", new String[]{"BASE TABLE"})) {
                    while (rs.next()) {
                        System.out.println(" - " + rs.getString("TABLE_NAME") + " (" + rs.getString("TABLE_TYPE") + ")");
                    }
                }

                // 4. Liste des colonnes d'une table
                String tableToInspect = "duckdb_settings"; // adapte au besoin
                System.out.println("\n=== Colonnes de la table: " + tableToInspect + " ===");
                try (ResultSet rs = meta.getColumns(null, null, tableToInspect, "%")) {
                    while (rs.next()) {
                        System.out.printf(" - %s (%s), nullable: %s\n",
                                rs.getString("COLUMN_NAME"),
                                rs.getString("TYPE_NAME"),
                                rs.getString("IS_NULLABLE"));
                    }
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
