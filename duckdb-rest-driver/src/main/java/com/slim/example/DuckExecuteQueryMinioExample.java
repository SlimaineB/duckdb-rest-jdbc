package com.slim.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class DuckExecuteQueryMinioExample {
    public static void main(String[] args) {
        try {
            // Chargement du driver (automatique si tu as bien le bloc static dans RestDriver)
            Class.forName("com.slim.duckdb.DuckDBDriver");

            // Connexion à ton serveur REST
            String url = "jdbc:duckdb://localhost:8080?useEncryption=false&disableCertificateVerification=true"; // Assure-toi que le port est correct
            Connection conn = DriverManager.getConnection(url);

            Statement stmt = conn.createStatement();
            
            // ResultSet rs = stmt.executeQuery("SELECT 1 AS id, 'hello' AS message");
            stmt.execute("SET s3_region='us-east-1'");
            stmt.execute("SET s3_url_style='path'");
            stmt.execute("SET s3_endpoint='localhost:9000'");
            stmt.execute("SET s3_access_key_id='minioadmin'");
            stmt.execute("SET s3_secret_access_key='minioadmin'");
            stmt.execute("SET s3_use_ssl=false");
            //ResultSet rs = stmt.executeQuery("SELECT * FROM read_parquet('s3://test-bucket/parquet_brut_big/fichier_1.parquet') order by id desc limit 5");
            // ResultSet rs = stmt.executeQuery("SELECT * FROM read_parquet('s3://test-bucket/data/init/part-00000-24117887-2b6f-443e-89cf-2ff06ed9a2ae-c000.snappy.parquet') order by id desc limit 5");
            ResultSet rs = stmt.executeQuery("SELECT TRUE AS bool_col, CAST(42 AS TINYINT) AS tinyint_col, CAST(255 AS UTINYINT) AS utinyint_col, CAST(-12345 AS SMALLINT) AS smallint_col, CAST(54321 AS USMALLINT) AS usmallint_col, CAST(123456 AS INTEGER) AS int_col, CAST(4000000000 AS UINTEGER) AS uint_col, CAST(9223372036854775807 AS BIGINT) AS bigint_col, CAST(18446744073709551615 AS UBIGINT) AS ubigint_col, CAST(3.14 AS FLOAT) AS float_col, CAST(2.718281828459 AS DOUBLE) AS double_col, CAST('2024-06-09 12:34:56.789' AS TIMESTAMP) AS ts_col");
            
            /*ResultSet rs = stmt.executeQuery(
                "SELECT " +
                "TIMESTAMPTZ '2024-06-09 12:34:56.789+02:00' AS ts_tz_col, " +
                "'{\"foo\": 123, \"bar\": [1,2,3]}'::JSON AS json_col, " +
                "CAST('68656c6c6f' AS BLOB) AS blob_col, " +
                "CAST('123e4567-e89b-12d3-a456-426614174000' AS UUID) AS uuid_col, " +
                "MAP([1,2],[3,4]) AS map_col, " +
                "[10,20,30] AS array_col, " +
                "STRUCT_PACK(a := 1, b := 'x') AS struct_col, " 
            );*/
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
