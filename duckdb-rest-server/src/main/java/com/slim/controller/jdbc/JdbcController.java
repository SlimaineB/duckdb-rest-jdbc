package com.slim.controller.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.duckdb.StatementReturnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.slim.dto.ExecuteDuckDBResultSetMetaData;
import com.slim.dto.ExecuteRequest;
import com.slim.dto.ExecuteResponse;

import javax.sql.DataSource;

/**
 * Contrôleur pour exécuter des requêtes SQL via JDBC.
 * Utilisé pour les requêtes envoyées par l'interface utilisateur.
 */
@RestController
@RequestMapping("/jdbc")
public class JdbcController {

    private static final Logger logger = LoggerFactory.getLogger(JdbcController.class);

    private final DataSource dataSource;

    @Autowired
    public JdbcController(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostMapping("/execute")
    public ResponseEntity<ExecuteResponse> execute(@RequestBody ExecuteRequest request) {
        logger.info("[DuckDBController] Reçu requête SQL: {}", request.sql);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(request.sql)) {

            // Appliquer les paramètres
            if (request.params != null) {
                logger.debug("Appliquer {} paramètres à la requête.", request.params.length);
                for (int i = 0; i < request.params.length; i++) {
                    stmt.setObject(i + 1, request.params[i]);
                }
            }

            boolean hasResultSet = stmt.execute();
            ExecuteDuckDBResultSetMetaData metadata;
            List<List<Object>> rows = new ArrayList<>();

            if (hasResultSet) {
                // Cas SELECT
                logger.info("Exécution d'une requête SELECT.");
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
                    }

                    logger.info("Résultat SELECT : {} lignes, {} colonnes.", rows.size(), colCount);

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
                    logger.info("Requête DML exécutée, lignes affectées : {}", updateCount);
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
                    rows.add(Arrays.asList(updateCount));
                } else {
                    logger.info("Requête DDL exécutée.");
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
                    rows.add(Arrays.asList("OK"));  // ou "DDL Executed"
                }
            }

            logger.debug("Réponse JDBC construite et renvoyée.");
            return ResponseEntity.ok(new ExecuteResponse(metadata, rows));

        } catch (Exception e) {
            logger.error("[DuckDBController] Erreur: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(new ExecuteResponse(e.getMessage()));
        }
    }
}
