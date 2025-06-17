package example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class DuckExecuteQueryNaNExample {
    public static void main(String[] args) {
        try {
            // Chargement du driver (automatique si tu as bien le bloc static dans RestDriver)
            Class.forName("com.slim.duckdb.DuckDBDriver");

            // Connexion à ton serveur REST
            String url = "jdbc:duckdb://localhost:8080?useEncryption=false&disableCertificateVerification=true"; // Assure-toi que le port est correct
            Connection conn = DriverManager.getConnection(url);

            Statement stmt = conn.createStatement();
            
            // ResultSet rs = stmt.executeQuery("SELECT 1 AS id, 'hello' AS message");
            ResultSet rs = stmt.executeQuery("SELECT 0.0 / 0.0 AS nan_value, 1.0 / 0.0 AS pos_inf, -1.0 / 0.0 AS neg_inf");
            //String query = "SELECT sum(1) AS 'cnt:test_EEEA2B98E81A4A8E857FAA47253B63E7:ok' FROM test";
            //ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();

            // Affichage des résultats
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    System.out.print(meta.getColumnName(i) + ": " + rs.getObject(i) + " \n ");
                }
                System.out.println();
            }


            System.out.println("---- SCHÉMAS ----");
            try (ResultSet rsSchemas = conn.getMetaData().getSchemas()) {
                while (rsSchemas.next()) {
                    System.out.println("Schéma: " + rsSchemas.getString(1));
                }
            }


            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
