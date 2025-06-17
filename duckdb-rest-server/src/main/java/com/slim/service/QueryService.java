package com.slim.service;

import com.slim.dto.ExecuteDuckDBResultSetMetaData;
import com.slim.dto.ExecuteRequest;
import com.slim.dto.ExecuteResponse;
import org.duckdb.StatementReturnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class QueryService {
    private static final Logger logger = LoggerFactory.getLogger(QueryService.class);

    private final DataSource dataSource;

    private final CacheService cacheService;

    public QueryService(DataSource dataSource, CacheService cacheService) {
        this.dataSource = dataSource;
        this.cacheService = cacheService;
    }

    public ExecuteResponse execute(ExecuteRequest request) {
        logger.info("[QueryService] Reçu requête SQL: {}", request.getSql());

        try (Connection connection = dataSource.getConnection()) {

            // 1. Vérifier le cache pour les SELECT sans paramètres
            if (cacheService.shouldUseCache(request.getSql()) && (request.getParams() == null || request.getParams().length == 0)) {
                List<String> cachedColumns = new ArrayList<>();
                List<String> cachedTypes = new ArrayList<>();
                List<String> cachedDetails = new ArrayList<>();
                List<List<Object>> cachedRows = cacheService.tryReadCache(connection, request.getSql(), cachedColumns, cachedTypes, cachedDetails);
                if (cachedRows != null) {
                    logger.info("Résultat SELECT récupéré depuis le cache.");
                    String[] columnNames = cachedColumns.toArray(new String[0]);
                    String[] columnTypes = cachedTypes.toArray(new String[0]);
                    String[] columnDetails = cachedDetails.toArray(new String[0]);
                    ExecuteDuckDBResultSetMetaData metadata = new ExecuteDuckDBResultSetMetaData(
                        0,
                        columnNames.length,
                        columnNames,
                        columnTypes,
                        columnDetails,
                        StatementReturnType.QUERY_RESULT.name(),
                        columnTypes,
                        columnDetails
                    );
                    return new ExecuteResponse(metadata, cachedRows);
                }
            }

            // 2. Exécution normale si pas de cache
            try (PreparedStatement stmt = connection.prepareStatement(request.getSql())) {
                if (request.getParams() != null) {
                    logger.debug("Appliquer {} paramètres à la requête.", request.getParams().length);
                    for (int i = 0; i < request.getParams().length; i++) {
                        stmt.setObject(i + 1, request.getParams()[i]);
                    }
                }

                boolean hasResultSet = stmt.execute();
                ExecuteDuckDBResultSetMetaData metadata;
                List<List<Object>> rows = new ArrayList<>();

                if (hasResultSet) {
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

                        // Mettre à jour le cache si applicable
                        if (cacheService.shouldUseCache(request.getSql()) && (request.getParams() == null || request.getParams().length == 0)) {
                            cacheService.performCache(connection, request.getSql());
                        }
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
                        rows.add(Arrays.asList("OK"));
                    }
                }

                logger.debug("Réponse JDBC construite et renvoyée.");
                return new ExecuteResponse(metadata, rows);
            }

        } catch (Exception e) {
            logger.error("[QueryService] Erreur: {}", e.getMessage(), e);
            return new ExecuteResponse(e.getMessage());
        }
    }
}
