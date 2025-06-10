package com.slim.server;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.duckdb.StatementReturnType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import ord.duckdb.dto.ExecuteDuckDBResultSetMetaData;
import ord.duckdb.dto.ExecuteRequest;
import ord.duckdb.dto.ExecuteResponse;  

@RestController
public class DuckDBController {

    private final Connection connection;

    public DuckDBController() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @PostMapping("/execute")

    public ResponseEntity<ExecuteResponse> execute(@RequestBody ExecuteRequest request) {
        System.out.println("[DuckDBController] Reçu requête SQL: " + request.sql);
        try (PreparedStatement stmt = connection.prepareStatement(request.sql)) {

            // Appliquer les paramètres
            if (request.params != null) {
                for (int i = 0; i < request.params.length; i++) {
                    stmt.setObject(i + 1, request.params[i]);
                }
            }

            boolean hasResultSet = stmt.execute();
            ExecuteDuckDBResultSetMetaData metadata;
            List<List<Object>> rows = new ArrayList<>();

            if (hasResultSet) {
                // Cas SELECT
                try (ResultSet rs = stmt.getResultSet()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int colCount = meta.getColumnCount();

                    String[] columnNames = new String[colCount];
                    String[] columnTypes = new String[colCount];
                    String[] columnDetails = new String[colCount];

                    for (int i = 0; i < colCount; i++) {
                        columnNames[i] = meta.getColumnName(i + 1);
                        columnTypes[i] = meta.getColumnTypeName(i + 1);
                        columnDetails[i] = meta.getColumnClassName(i + 1);
                    }

                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();
                        for (int i = 1; i <= colCount; i++) {
                            row.add(rs.getObject(i));
                        }
                        rows.add(row);
                        System.out.println("Row: " + row);
                    }

                    metadata = new ExecuteDuckDBResultSetMetaData(
                        stmt.getParameterMetaData().getParameterCount(),
                        colCount,
                        columnNames,
                        columnTypes,
                        columnDetails,
                        StatementReturnType.QUERY_RESULT.name(),
                        columnTypes,
                        columnDetails
                    );
                }
            } else {
                // Cas UPDATE/INSERT/DELETE ou DDL
                int updateCount = stmt.getUpdateCount();

                if (updateCount >= 0) {
                    // C'est une requête DML (affecte des lignes)
                    metadata = new ExecuteDuckDBResultSetMetaData(
                        stmt.getParameterMetaData().getParameterCount(),
                        1,
                        new String[]{"affected_rows"},
                        new String[]{"INTEGER"},
                        new String[]{"java.lang.Integer"},
                        StatementReturnType.CHANGED_ROWS.name(),
                        new String[]{"INTEGER"},
                        new String[]{"java.lang.Integer"}
                    );
                    rows.add(List.of(updateCount));
                } else {
                    // Cas DDL (CREATE, DROP, etc)
                    metadata = new ExecuteDuckDBResultSetMetaData(
                        stmt.getParameterMetaData().getParameterCount(),
                        1,
                        new String[]{"status"},
                        new String[]{"VARCHAR"},
                        new String[]{"java.lang.String"},
                        StatementReturnType.NOTHING.name(),
                        new String[]{"VARCHAR"},
                        new String[]{"java.lang.String"}
                    );
                    rows.add(List.of("OK"));  // ou "DDL Executed"
                }
            }

            return ResponseEntity.ok(new ExecuteResponse(metadata, rows));

        } catch (Exception e) {
            System.err.println("[DuckDBController] Erreur: " + e.getMessage());
            e.printStackTrace();
            return ResponseEntity.status(500).body(new ExecuteResponse(e.getMessage()));
        }
    }



    
}
