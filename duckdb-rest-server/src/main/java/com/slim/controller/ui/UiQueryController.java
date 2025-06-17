package com.slim.controller.ui;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import javax.sql.DataSource;
import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/ui")
public class UiQueryController {

    private static final Logger logger = LoggerFactory.getLogger(UiQueryController.class);

    @Autowired
    private DataSource dataSource;

    @PostMapping("/query")
    public ResponseEntity<?> executeQuery(@RequestBody Map<String, Object> req) {
        String query = ((String) req.get("query")).trim().replaceAll(";$", "");
        int maxRows = req.get("max_rows") != null ? ((Number) req.get("max_rows")).intValue() : 1000;
        int numThreads = req.get("num_threads") != null ? ((Number) req.get("num_threads")).intValue() : -1;
        boolean profiling = req.get("profiling") != null && (Boolean) req.get("profiling");
        boolean forceRefreshCache = req.get("force_refresh_cache") != null && (Boolean) req.get("force_refresh_cache");

        String hostname = getHostname();
        long startTime = System.currentTimeMillis();
        String originalThreads = null;

        logger.info("Received query from {}", hostname);
        logger.info("Threads requested: {}", numThreads);
        logger.info("Query:\n{}", query);

        try (Connection con = dataSource.getConnection()) {
            // Gestion threads
            if (numThreads != -1) {
                try (Statement st = con.createStatement()) {
                    ResultSet rs = st.executeQuery("SELECT current_setting('threads') AS val");
                    if (rs.next()) originalThreads = rs.getString(1);
                    st.execute("SET threads TO " + numThreads);
                    logger.info("Threads set to {} (original was {})", numThreads, originalThreads);
                } catch (Exception e) {
                    logger.warn("Failed to set threads: {}", e.getMessage());
                }
            }

            // Ajout LIMIT si SELECT sans LIMIT
            if (Pattern.compile("(?i)^select\\b").matcher(query).find() &&
                !Pattern.compile("(?i)\\blimit\\b").matcher(query).find()) {
                query += " LIMIT " + maxRows;
                logger.info("Appended LIMIT {}", maxRows);
            }

            // Profiling
            if (profiling) {
                String profilePath = "/tmp/duckdb_profile_" + UUID.randomUUID() + ".json";
                try (Statement st = con.createStatement()) {
                    st.execute("SET enable_profiling = 'json'");
                    st.execute("SET profiling_output = '" + profilePath + "'");
                    File f = new File(profilePath);
                    if (f.exists()) f.delete();
                    st.execute(query);
                    long waitStart = System.currentTimeMillis();
                    while (!f.exists()) {
                        if (System.currentTimeMillis() - waitStart > 2000)
                            return ResponseEntity.status(500).body(Collections.singletonMap("error", "Profiling file not written."));
                        Thread.sleep(10);
                    }
                    String profilingData = new String(Files.readAllBytes(f.toPath()), "UTF-8");
                    f.delete();
                    double execTime = (System.currentTimeMillis() - startTime) / 1000.0;
                    Map<String, Object> resp = new HashMap<>();
                    resp.put("profiling", new com.fasterxml.jackson.databind.ObjectMapper().readValue(profilingData, Map.class));
                    resp.put("hostname", hostname);
                    resp.put("execution_time", execTime);
                    logger.info("Profiling completed in {} seconds", execTime);
                    return ResponseEntity.ok(resp);
                }
            }

            // Gestion du cache
            List<String> columns = new ArrayList<>();
            List<List<Object>> rows = null;
            if (shouldUseCache(query) && !forceRefreshCache) {
                logger.info("Trying to read cache for query...");
                rows = tryReadCache(con, query, columns);
                if (rows != null) {
                    logger.info("Cache hit for query.");
                    double execTime = (System.currentTimeMillis() - startTime) / 1000.0;
                    Map<String, Object> resp = new HashMap<>();
                    resp.put("columns", columns);
                    resp.put("rows", rows);
                    resp.put("hostname", hostname);
                    resp.put("execution_time", execTime);
                    return ResponseEntity.ok(resp);
                } else {
                    logger.info("No valid cache found for query.");
                }
            }

            // Exécution SQL
            logger.info("Executing SQL query...");
            try (PreparedStatement stmt = con.prepareStatement(query)) {
                boolean hasResultSet = stmt.execute();
                columns = new ArrayList<>();
                rows = new ArrayList<>();
                if (hasResultSet) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        ResultSetMetaData meta = rs.getMetaData();
                        int colCount = meta.getColumnCount();
                        for (int i = 1; i <= colCount; i++) columns.add(meta.getColumnName(i));
                        while (rs.next()) {
                            List<Object> row = new ArrayList<>();
                            for (int i = 1; i <= colCount; i++) row.add(sanitizeValue(rs.getObject(i)));
                            rows.add(row);
                        }
                    }
                }
            }
            double execTime = (System.currentTimeMillis() - startTime) / 1000.0;
            logger.info("Returned {} rows in {} seconds", rows != null ? rows.size() : 0, execTime);
            // Cache si lent
            if (execTime > 0.005) {
                logger.info("Query duration > 0.5s, caching result...");
                performCache(con, query);
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("columns", columns);
            resp.put("rows", rows);
            resp.put("hostname", hostname);
            resp.put("execution_time", execTime);
            return ResponseEntity.ok(resp);

        } catch (Exception e) {
            logger.error("Query execution failed: {}", e.getMessage());
            return ResponseEntity.status(400).body(Collections.singletonMap("error", e.getMessage()));
        }
    }

    private Object sanitizeValue(Object val) {
        if (val == null) return null;
        if (val instanceof Float || val instanceof Double) {
            double d = ((Number) val).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) return null;
            return d;
        }
        if (val instanceof Integer || val instanceof Long || val instanceof String || val instanceof Boolean)
            return val;
        if (val instanceof BigDecimal || val instanceof java.sql.Date || val instanceof java.sql.Timestamp)
            return val.toString();
        if (val instanceof LocalDate || val instanceof LocalDateTime)
            return val.toString();
        return val.toString();
    }

    private String getHostname() {
        try { return java.net.InetAddress.getLocalHost().getHostName(); }
        catch (Exception e) { return "unknown"; }
    }

    // --- Adaptation de la gestion du cache façon Python ---

    private static final String CACHE_OUTPUT_BASE = System.getenv().getOrDefault("CACHE_OUTPUT_BASE", "./db_cache");
    private static final int MAX_CACHE_AGE_MINUTES = Integer.parseInt(System.getenv().getOrDefault("CACHE_TTL_MINUTES", "60"));

    private String getParquetCachePath(String query) {
        try {
            String normalizedQuery = query.trim().replaceAll(";$", "");
            String queryHash = sha256(normalizedQuery);
            String cachedDate = java.time.LocalDate.now().toString();
            return CACHE_OUTPUT_BASE.replaceAll("/$", "") + "/cached_date=" + cachedDate + "/db_cache_" + queryHash + ".parquet";
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute cache path", e);
        }
    }

    private String sha256(String base) throws Exception {
        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(base.getBytes("UTF-8"));
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if(hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }


    private List<List<Object>> tryReadCache(Connection con, String query, List<String> columnsOut) {
        String parquetPath = getParquetCachePath(query);
        logger.info("Attempting to read cache from {}", parquetPath);
        String sql = "SELECT * EXCLUDE (cached_at, cached_date) FROM read_parquet('" + parquetPath + "') " +
                     "WHERE cached_at >= NOW() - INTERVAL '" + MAX_CACHE_AGE_MINUTES + " minutes'";
        try (Statement st = con.createStatement()) {
            ResultSet rs = st.executeQuery(sql);
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            columnsOut.clear();
            for (int i = 1; i <= colCount; i++) {
                columnsOut.add(meta.getColumnName(i));
            }
            List<List<Object>> rows = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= colCount; i++) {
                    row.add(sanitizeValue(rs.getObject(i)));
                }
                rows.add(row);
            }
            if (!rows.isEmpty()) {
                logger.info("Cache hit: returning cached result for query.");
                return rows;
            } else {
                logger.info("Cache file found but empty or expired for query.");
            }
        } catch (Exception e) {
            logger.info("No valid cache found or failed to read at {}: {}", parquetPath, e.getMessage());
        }
        return null;
    }

    private void performCache(Connection con, String query) {
        String parquetPath = getParquetCachePath(query);
        boolean isS3 = parquetPath.contains("://");
        File outFile = new File(parquetPath);
        if (!isS3) {
            outFile.getParentFile().mkdirs();
        }
        String cacheSql = "COPY (SELECT subq.*, NOW() AS cached_at FROM (" + query + ") AS subq) TO '" +
                parquetPath + "' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)";
        try (Statement st = con.createStatement()) {
            logger.info("Caching query result to {}", parquetPath);
            st.execute(cacheSql);
            logger.info("Cached result to {}", parquetPath);
        } catch (Exception e) {
            logger.warn("Failed to cache query to {}: {}", parquetPath, e.getMessage());
        }
    }

    // Cache helpers (à adapter selon ton infra)
    private boolean shouldUseCache(String query) {
        return query.trim().toLowerCase().startsWith("select");
    }
}
