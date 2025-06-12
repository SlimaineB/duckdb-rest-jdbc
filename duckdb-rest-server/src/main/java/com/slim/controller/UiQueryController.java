package com.slim.controller;

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

        try (Connection con = dataSource.getConnection()) {
            // Gestion threads
            if (numThreads != -1) {
                try (Statement st = con.createStatement()) {
                    ResultSet rs = st.executeQuery("SELECT current_setting('threads') AS val");
                    if (rs.next()) originalThreads = rs.getString(1);
                    st.execute("SET threads TO " + numThreads);
                } catch (Exception e) {
                    // log warning
                }
            }

            // Ajout LIMIT si SELECT sans LIMIT
            if (Pattern.compile("(?i)^select\\b").matcher(query).find() &&
                !Pattern.compile("(?i)\\blimit\\b").matcher(query).find()) {
                query += " LIMIT " + maxRows;
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
                    return ResponseEntity.ok(resp);
                }
            }

            // Gestion du cache
            List<List<Object>> rows;
            List<String> columns;
            if (shouldUseCache(query) && !forceRefreshCache) {
                List<Object> cacheResult = tryReadCache(con, query);
                if (cacheResult != null) {
                    Map<String, Object> cacheMap = (Map<String, Object>) cacheResult.get(0);
                    columns = new ArrayList<>(cacheMap.keySet());
                    rows = new ArrayList<>();
                    for (Object rowObj : cacheResult) {
                        Map<String, Object> rowMap = (Map<String, Object>) rowObj;
                        List<Object> row = new ArrayList<>();
                        for (String col : columns) row.add(sanitizeValue(rowMap.get(col)));
                        rows.add(row);
                    }
                    double execTime = (System.currentTimeMillis() - startTime) / 1000.0;
                    Map<String, Object> resp = new HashMap<>();
                    resp.put("columns", columns);
                    resp.put("rows", rows);
                    resp.put("hostname", hostname);
                    resp.put("execution_time", execTime);
                    return ResponseEntity.ok(resp);
                }
            }

            // Exécution SQL
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
            // Cache si lent
            if (execTime > 0.5) performCache(con, query);

            Map<String, Object> resp = new HashMap<>();
            resp.put("columns", columns);
            resp.put("rows", rows);
            resp.put("hostname", hostname);
            resp.put("execution_time", execTime);
            return ResponseEntity.ok(resp);

        } catch (Exception e) {
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

    // Cache helpers (à adapter selon ton infra)
    private boolean shouldUseCache(String query) {
        return query.trim().toLowerCase().startsWith("select");
    }

    private List<Object> tryReadCache(Connection con, String query) {
        // À implémenter selon ta logique de cache parquet
        return null;
    }

    private void performCache(Connection con, String query) {
        // À implémenter selon ta logique de cache parquet
    }
}
