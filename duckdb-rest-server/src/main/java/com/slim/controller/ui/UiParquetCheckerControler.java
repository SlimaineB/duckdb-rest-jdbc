package com.slim.controller.ui;

import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.regex.*;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/ui/parquet_checker")
public class UiParquetCheckerControler {

    private final DataSource dataSource;

    public UiParquetCheckerControler(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostMapping("/check_parquet_file_size")
    public ResponseEntity<?> checkParquetFileSize(@RequestBody Map<String, Object> req) {
        String s3Path = (String) req.get("s3_path");
        int cpuCount = Runtime.getRuntime().availableProcessors();

        try (Connection con = dataSource.getConnection();
             Statement st = con.createStatement()) {

            String query = String.format(
                "SELECT file_name, COUNT(DISTINCT row_group_id) AS row_group_count, " +
                "SUM(row_group_num_rows) AS total_rows, " +
                "ROUND(SUM(total_compressed_size) / 1024.0 / 1024.0, 2) AS compressed_file_size_mb, " +
                "ROUND(SUM(total_uncompressed_size) / 1024.0 / 1024.0, 2) AS uncompressed_file_size_mb, " +
                "CASE " +
                "WHEN ROUND(SUM(total_compressed_size) / 1024.0 / 1024.0, 2) < 100 THEN 'Too small ❌' " +
                "WHEN ROUND(SUM(total_compressed_size) / 1024.0 / 1024.0, 2) > 10240 THEN 'Too big ⚠️' " +
                "ELSE 'Optimal ✅' END AS quality " +
                "FROM parquet_metadata('%s') GROUP BY file_name ORDER BY compressed_file_size_mb DESC", s3Path);

            ResultSet rs = st.executeQuery(query);
            ResultSetMetaData meta = rs.getMetaData();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) columns.add(meta.getColumnName(i));

            List<Map<String, Object>> files = new ArrayList<>();
            int totalRowGroups = 0;
            while (rs.next()) {
                Map<String, Object> fileData = new LinkedHashMap<>();
                for (int i = 1; i <= columns.size(); i++) {
                    fileData.put(columns.get(i - 1), rs.getObject(i));
                }
                int rgCount = ((Number) fileData.get("row_group_count")).intValue();
                totalRowGroups += rgCount;
                String parallelismQuality = rgCount == cpuCount ? "✅ Optimal"
                        : (rgCount < cpuCount ? "❌ Underutilized" : "⚠️ Overhead Risk");
                fileData.put("parallelism_quality", parallelismQuality);
                files.add(fileData);
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("s3_path", s3Path);
            resp.put("total_row_groups", totalRowGroups);
            resp.put("cpu_count", cpuCount);
            resp.put("files", files);
            return ResponseEntity.ok(resp);

        } catch (Exception e) {
            return ResponseEntity.status(400).body(Collections.singletonMap("error", e.getMessage()));
        }
    }

    @PostMapping("/check_parquet_row_group_size")
    public ResponseEntity<?> checkRowGroupSize(@RequestBody Map<String, Object> req) {
        String s3Path = (String) req.get("s3_path");
        try (Connection con = dataSource.getConnection();
             Statement st = con.createStatement()) {

            String query = String.format(
                "SELECT file_name, row_group_id, row_group_num_rows, " +
                "ROUND(row_group_bytes / 1024.0, 2) AS size_kb, " +
                "CASE " +
                "WHEN row_group_num_rows < 5000 THEN 'Very small ❌' " +
                "WHEN row_group_num_rows < 20000 THEN 'Suboptimal ⚠️' " +
                "WHEN row_group_num_rows BETWEEN 100000 AND 1000000 THEN 'Optimal ✅' " +
                "WHEN row_group_num_rows >= 1000000 THEN 'Too high ⚠️' " +
                "ELSE 'okay' END AS quality " +
                "FROM parquet_metadata('%s') " +
                "GROUP BY file_name, row_group_id, row_group_num_rows, row_group_bytes " +
                "ORDER BY size_kb DESC", s3Path);

            ResultSet rs = st.executeQuery(query);
            ResultSetMetaData meta = rs.getMetaData();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) columns.add(meta.getColumnName(i));

            List<Map<String, Object>> rowGroups = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> rowData = new LinkedHashMap<>();
                for (int i = 1; i <= columns.size(); i++) {
                    rowData.put(columns.get(i - 1), rs.getObject(i));
                }
                rowGroups.add(rowData);
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("s3_path", s3Path);
            resp.put("row_groups", rowGroups);
            return ResponseEntity.ok(resp);

        } catch (Exception e) {
            return ResponseEntity.status(400).body(Collections.singletonMap("error", e.getMessage()));
        }
    }

    // Utilitaire pour extraire les colonnes de partition d'un chemin S3
    private Set<String> extractPartitionColumnsFromPath(String s3Path) throws UnsupportedEncodingException {
        String decoded = URLDecoder.decode(s3Path, "UTF-8");
        Set<String> partitions = new HashSet<>();
        Matcher m = Pattern.compile("/([^/=]+)=").matcher(decoded);
        while (m.find()) {
            partitions.add(m.group(1));
        }
        return partitions;
    }

    @PostMapping("/suggest_partitions")
    public ResponseEntity<?> suggestPartitions(@RequestBody Map<String, Object> req) {
        String s3Path = (String) req.get("s3_path");
        int threshold = req.get("threshold") != null ? ((Number) req.get("threshold")).intValue() : 50;
        try (Connection con = dataSource.getConnection();
             Statement st = con.createStatement()) {

            st.execute("CREATE OR REPLACE VIEW parquet_data AS SELECT * FROM parquet_scan('" + s3Path + "');");
            Set<String> existingPartitions = extractPartitionColumnsFromPath(s3Path);

            ResultSet colRs = st.executeQuery("PRAGMA table_info(parquet_data);");
            List<String> columnNames = new ArrayList<>();
            while (colRs.next()) {
                columnNames.add(colRs.getString(2));
            }

            List<String> suggestions = new ArrayList<>();
            List<Map<String, Object>> result = new ArrayList<>();

            for (String colName : columnNames) {
                boolean alreadyPartitioned = existingPartitions.contains(colName);
                try {
                    ResultSet cardRs = st.executeQuery("SELECT COUNT(DISTINCT " + colName + ") FROM parquet_data;");
                    int cardinality = cardRs.next() ? cardRs.getInt(1) : 0;

                    ResultSet topValRs = st.executeQuery(
                        "SELECT MAX(cnt) * 1.0 / SUM(cnt) FROM (SELECT COUNT(*) as cnt FROM parquet_data GROUP BY " + colName + ");"
                    );
                    double topValRatio = topValRs.next() ? topValRs.getDouble(1) : 1.0;
                    boolean isBalanced = topValRatio < 0.7;

                    String suggest;
                    if (alreadyPartitioned) {
                        suggest = "🔁";
                    } else if (cardinality <= threshold && isBalanced) {
                        suggest = "✅";
                        suggestions.add(colName);
                    } else if (!isBalanced) {
                        suggest = "⚠️ Unbalanced";
                    } else {
                        suggest = "❌";
                    }

                    Map<String, Object> colInfo = new LinkedHashMap<>();
                    colInfo.put("column", colName);
                    colInfo.put("distinct_values", cardinality);
                    colInfo.put("top_value_ratio", Math.round(topValRatio * 100.0) / 100.0);
                    colInfo.put("balanced", isBalanced);
                    colInfo.put("suggest", suggest);
                    colInfo.put("already_partitioned", alreadyPartitioned);
                    result.add(colInfo);

                } catch (Exception ex) {
                    Map<String, Object> colInfo = new LinkedHashMap<>();
                    colInfo.put("column", colName);
                    colInfo.put("distinct_values", "error");
                    colInfo.put("top_value_ratio", null);
                    colInfo.put("balanced", false);
                    colInfo.put("suggest", "⚠️");
                    colInfo.put("already_partitioned", alreadyPartitioned);
                    result.add(colInfo);
                }
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("s3_path", s3Path);
            resp.put("threshold", threshold);
            resp.put("already_partitioned_columns", new ArrayList<>(existingPartitions));
            resp.put("columns", result);
            resp.put("suggested_partitions", suggestions);
            return ResponseEntity.ok(resp);

        } catch (Exception e) {
            return ResponseEntity.status(400).body(Collections.singletonMap("error", e.getMessage()));
        }
    }

    @PostMapping("/partition_value_counts")
    public ResponseEntity<?> getPartitionValueCounts(@RequestBody Map<String, Object> req) {
        String s3Path = (String) req.get("s3_path");
        String column = (String) req.get("column");
        try (Connection con = dataSource.getConnection();
             Statement st = con.createStatement()) {

            st.execute("CREATE OR REPLACE VIEW parquet_data AS SELECT * FROM parquet_scan('" + s3Path + "');");
            ResultSet rs = st.executeQuery(
                "SELECT " + column + " AS value, COUNT(*) AS count FROM parquet_data GROUP BY " + column + " ORDER BY count DESC"
            );
            List<Map<String, Object>> result = new ArrayList<>();
            int sumCount = 0;
            List<Object[]> rows = new ArrayList<>();
            while (rs.next()) {
                Object[] row = new Object[]{rs.getObject(1), rs.getInt(2)};
                rows.add(row);
                sumCount += rs.getInt(2);
            }
            for (Object[] row : rows) {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put("value", row[0]);
                map.put("count", row[1]);
                map.put("repartion", sumCount > 0 ? (Math.round(((int) row[1]) * 100.0 / sumCount) + "%") : "0%");
                result.add(map);
            }
            return ResponseEntity.ok(Collections.singletonMap("counts", result));
        } catch (Exception e) {
            return ResponseEntity.status(400).body(Collections.singletonMap("error", e.getMessage()));
        }
    }

    @PostMapping("/parquet_filterability_score")
    public ResponseEntity<?> parquetFilterabilityScore(@RequestBody Map<String, Object> req) {
        String s3Path = (String) req.get("s3_path");
        try (Connection con = dataSource.getConnection();
             Statement st = con.createStatement()) {

            st.execute("CREATE OR REPLACE VIEW parquet_data AS SELECT * FROM parquet_scan('" + s3Path + "');");
            ResultSet colRs = st.executeQuery("PRAGMA table_info(parquet_data);");
            List<String> colNames = new ArrayList<>();
            while (colRs.next()) colNames.add(colRs.getString(2));

            List<Map<String, Object>> results = new ArrayList<>();
            for (String col : colNames) {
                try {
                    ResultSet distinctRs = st.executeQuery("SELECT COUNT(DISTINCT " + col + ") FROM parquet_data");
                    int distinctCount = distinctRs.next() ? distinctRs.getInt(1) : 0;

                    ResultSet topValRs = st.executeQuery(
                        "SELECT MAX(cnt) * 1.0 / SUM(cnt) FROM (SELECT COUNT(*) AS cnt FROM parquet_data GROUP BY " + col + ")"
                    );
                    double topValRatio = topValRs.next() ? topValRs.getDouble(1) : 1.0;

                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put("column", col);
                    map.put("distinct_values", distinctCount);
                    map.put("top_value_ratio", Math.round(topValRatio * 100.0) / 100.0);
                    results.add(map);
                } catch (Exception ex) {
                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put("column", col);
                    map.put("distinct_values", null);
                    map.put("top_value_ratio", null);
                    results.add(map);
                }
            }

            // Bloom filter metadata
            ResultSet bfRs = st.executeQuery(
                "SELECT path_in_schema AS column, COUNT(*) AS num_row_groups, " +
                "SUM(CASE WHEN bloom_filter_offset IS NOT NULL AND bloom_filter_length > 0 THEN 1 ELSE 0 END) AS num_with_bloom, " +
                "SUM(CASE WHEN bloom_filter_offset IS NOT NULL AND bloom_filter_length = 0 THEN 1 ELSE 0 END) AS num_declared_but_empty, " +
                "SUM(CASE WHEN bloom_filter_offset IS NOT NULL AND bloom_filter_length IS NULL THEN 1 ELSE 0 END) AS num_declared_but_length_missing, " +
                "ROUND(SUM(CASE WHEN bloom_filter_offset IS NOT NULL AND bloom_filter_length > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS bloom_coverage " +
                "FROM parquet_metadata('" + s3Path + "') WHERE path_in_schema IS NOT NULL GROUP BY path_in_schema"
            );
            Map<String, Map<String, Object>> bfInfo = new HashMap<>();
            while (bfRs.next()) {
                Map<String, Object> map = new HashMap<>();
                map.put("num_row_groups", bfRs.getInt("num_row_groups"));
                map.put("num_with_bloom", bfRs.getInt("num_with_bloom"));
                map.put("num_declared_but_empty", bfRs.getInt("num_declared_but_empty"));
                map.put("num_declared_but_length_missing", bfRs.getInt("num_declared_but_length_missing"));
                map.put("bloom_coverage", bfRs.getDouble("bloom_coverage"));
                bfInfo.put(bfRs.getString("column"), map);
            }

            // Merge & Score
            for (Map<String, Object> r : results) {
                String col = (String) r.get("column");
                Map<String, Object> bloomData = bfInfo.getOrDefault(col, new HashMap<>());
                double coverage = bloomData.get("bloom_coverage") != null ? ((Number) bloomData.get("bloom_coverage")).doubleValue() : 0.0;
                int cardinality = r.get("distinct_values") != null ? ((Number) r.get("distinct_values")).intValue() : 0;
                double topRatio = r.get("top_value_ratio") != null ? ((Number) r.get("top_value_ratio")).doubleValue() : 1.0;

                int score = 0;
                if (coverage > 0) score++;
                if (cardinality > 50) score++;
                if (topRatio < 0.5) score++;

                r.put("bloom_filter_coverage_percent", coverage);
                r.put("row_groups_with_bloom", bloomData.getOrDefault("num_with_bloom", 0));
                r.put("row_groups_declared_but_empty", bloomData.getOrDefault("num_declared_but_empty", 0));
                r.put("row_groups_declared_but_length_missing", bloomData.getOrDefault("num_declared_but_length_missing", 0));
                r.put("filterability_score", score);
                r.put("filterability_label", score == 3 ? "✅ High" : (score == 2 ? "🟡 Medium" : "⚠️ Low"));
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("s3_path", s3Path);
            resp.put("columns", results);
            return ResponseEntity.ok(resp);

        } catch (Exception e) {
            return ResponseEntity.status(400).body(Collections.singletonMap("error", e.getMessage()));
        }
    }

    @PostMapping("/parquet_bloom_filter_check")
    public ResponseEntity<?> checkBloomFilter(@RequestBody Map<String, Object> req) {
        String s3Path = (String) req.get("s3_path");
        try (Connection con = dataSource.getConnection();
             Statement st = con.createStatement()) {

            String query = String.format(
                "SELECT file_name, row_group_id, path_in_schema AS column, " +
                "bloom_filter_offset IS NOT NULL AND bloom_filter_length > 0 AS has_bloom_filter, " +
                "bloom_filter_offset, bloom_filter_length, " +
                "CASE WHEN bloom_filter_offset IS NOT NULL THEN " +
                "CASE WHEN bloom_filter_length > 0 THEN '✅ Present' ELSE '⚠️ Declared but empty' END " +
                "ELSE '❌ Absent' END AS status " +
                "FROM parquet_metadata('%s') WHERE path_in_schema IS NOT NULL ORDER BY file_name, path_in_schema", s3Path);

            ResultSet rs = st.executeQuery(query);
            ResultSetMetaData meta = rs.getMetaData();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) columns.add(meta.getColumnName(i));

            Map<String, Map<String, List<String>>> grouped = new HashMap<>();
            while (rs.next()) {
                Map<String, Object> rowDict = new HashMap<>();
                for (int i = 1; i <= columns.size(); i++) {
                    rowDict.put(columns.get(i - 1), rs.getObject(i));
                }
                String file = (String) rowDict.get("file_name");
                String col = (String) rowDict.get("column");
                String status = (String) rowDict.get("status");
                grouped.computeIfAbsent(file, k -> new HashMap<>())
                        .computeIfAbsent(col, k -> new ArrayList<>())
                        .add(status);
            }

            List<Map<String, Object>> summary = new ArrayList<>();
            for (Map.Entry<String, Map<String, List<String>>> fileEntry : grouped.entrySet()) {
                String file = fileEntry.getKey();
                for (Map.Entry<String, List<String>> colEntry : fileEntry.getValue().entrySet()) {
                    String col = colEntry.getKey();
                    List<String> statuses = colEntry.getValue();
                    int numRowGroups = statuses.size();
                    int numWithBloom = Collections.frequency(statuses, "✅ Present");
                    int numDeclaredButEmpty = Collections.frequency(statuses, "⚠️ Declared but empty");
                    int numAbsent = Collections.frequency(statuses, "❌ Absent");
                    double presenceRatio = numRowGroups > 0 ? Math.round(numWithBloom * 1000.0 / numRowGroups) / 10.0 : 0.0;
                    double declaredButEmptyRatio = numRowGroups > 0 ? Math.round(numDeclaredButEmpty * 1000.0 / numRowGroups) / 10.0 : 0.0;
                    String status;
                    if (numWithBloom == numRowGroups) status = "✅ Fully Present";
                    else if (numDeclaredButEmpty > 0 && numWithBloom > 0) status = "⚠️ Some Empty";
                    else if (numDeclaredButEmpty == numRowGroups) status = "⚠️ Declared but Empty";
                    else status = "❌ Absent";
                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put("file", file);
                    map.put("column", col);
                    map.put("num_row_groups", numRowGroups);
                    map.put("num_with_bloom", numWithBloom);
                    map.put("num_declared_but_empty", numDeclaredButEmpty);
                    map.put("num_absent", numAbsent);
                    map.put("presence_ratio", presenceRatio);
                    map.put("declared_but_empty_ratio", declaredButEmptyRatio);
                    map.put("status", status);
                    summary.add(map);
                }
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("s3_path", s3Path);
            resp.put("columns", summary);
            return ResponseEntity.ok(resp);

        } catch (Exception e) {
            return ResponseEntity.status(400).body(Collections.singletonMap("error", e.getMessage()));
        }
    }

    @GetMapping("/s3_test")
    public ResponseEntity<?> testS3Connection() {
        try (Connection con = dataSource.getConnection();
             Statement st = con.createStatement()) {
            st.executeQuery("SELECT * FROM list('s3://your-bucket/') LIMIT 1;");
            return ResponseEntity.ok(Collections.singletonMap("s3", "ok"));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Collections.singletonMap("error", "S3 config error: " + e.getMessage()));
        }
    }
}
