package org.duckdb.client;

import java.sql.SQLException;

import org.duckdb.dto.ExecuteRequest;
import org.duckdb.dto.ExecuteResponse;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class DuckDBHttpClient {

    private String backendUrl = "http://localhost:8080/";

    private final RestTemplate restTemplate = new RestTemplate();

    public DuckDBHttpClient(String backendUrl) {
        if (backendUrl != null && !backendUrl.isEmpty()) {
            this.backendUrl = backendUrl;
        }
    }

    public ExecuteResponse execute(String sql, Object[] params) throws SQLException {
        try {
            String executeUrl = this.backendUrl+"/execute"; // Direct access to DuckDB REST server

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            ExecuteRequest request = new ExecuteRequest(sql, params);
            HttpEntity<ExecuteRequest> entity = new HttpEntity<>(request, headers);

            ResponseEntity<ExecuteResponse> response = restTemplate.exchange(
                    executeUrl,
                    HttpMethod.POST,
                    entity,
                    ExecuteResponse.class
            );

            return response.getBody();

        } catch (Exception e) {
            throw new SQLException("Erreur lors de l'appel REST vers /execute", e);
        }
    }
}
