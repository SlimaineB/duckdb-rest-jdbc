package com.slim.service;

import java.io.File;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class CacheService {
        // --- Cache config (copié de UiQueryController) ---
        
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);
    private static final String CACHE_OUTPUT_BASE = System.getenv().getOrDefault("CACHE_OUTPUT_BASE", "./db_cache");
    private static final int MAX_CACHE_AGE_MINUTES = Integer.parseInt(System.getenv().getOrDefault("CACHE_TTL_MINUTES", "60"));

    // --- Ajout gestion du cache ---
    private String getParquetCachePath(String query) {
        try {
            String normalizedQuery = query.trim().replaceAll(";$", "");
            String queryHash = sha256(normalizedQuery);
            String cachedDate = LocalDate.now().toString();
            return CACHE_OUTPUT_BASE.replaceAll("/$", "") + "/cached_date=" + cachedDate + "/db_cache_" + queryHash + ".parquet";
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute cache path", e);
        }
    }

    private String sha256(String base) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(base.getBytes("UTF-8"));
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if(hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }


    public List<List<Object>> tryReadCache(Connection con, String query, List<String> columnsOut, List<String> columnTypesOut, List<String> columnDetailsOut) {
        String parquetPath = getParquetCachePath(query);
        String sql = "SELECT * EXCLUDE (cached_at, cached_date) FROM read_parquet('" + parquetPath + "') " +
                     "WHERE cached_at >= NOW() - INTERVAL '" + MAX_CACHE_AGE_MINUTES + " minutes'";
        try (Statement st = con.createStatement()) {
            ResultSet rs = st.executeQuery(sql);
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            columnsOut.clear();
            columnTypesOut.clear();
            columnDetailsOut.clear();
            for (int i = 1; i <= colCount; i++) {
                columnsOut.add(meta.getColumnName(i));
                columnTypesOut.add(meta.getColumnTypeName(i));
                columnDetailsOut.add(meta.getColumnClassName(i));
            }
            List<List<Object>> rows = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= colCount; i++) {
                    row.add(rs.getObject(i));
                }
                rows.add(row);
            }
            if (!rows.isEmpty()) {
                return rows;
            }
        }
         catch (SQLException sqle) {
            logger.warn("Cache read failed for query '{}': {}", query, sqle.getMessage());
        }
        catch (Exception e) {
            logger.warn("Cache read failed for query '{}': {}", query, e.getMessage(), e);
        }
        return null;
    }

    public void performCache(Connection con, String query) {
        String parquetPath = getParquetCachePath(query);
        boolean isS3 = parquetPath.contains("://");
        File outFile = new File(parquetPath);
        if (!isS3) {
            outFile.getParentFile().mkdirs();
        }
        String cacheSql = "COPY (SELECT subq.*, NOW() AS cached_at FROM (" + query + ") AS subq) TO '" +
                parquetPath + "' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)";
        try (Statement st = con.createStatement()) {
            st.execute(cacheSql);
        } catch (Exception e) {
            // TODO
            logger.error("Failed to cache query results: {}", e.getMessage(), e);
        }
    }

    public boolean shouldUseCache(String query) {
        return query.trim().toLowerCase().startsWith("select") && MAX_CACHE_AGE_MINUTES > 0 && !CACHE_OUTPUT_BASE.isEmpty();
    }
}
