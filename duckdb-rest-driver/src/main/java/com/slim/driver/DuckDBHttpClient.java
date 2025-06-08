package com.slim.driver;

import java.sql.SQLException;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.slim.dto.ExecuteRequest;
import com.slim.dto.ExecuteResponse;

public class DuckDBHttpClient {

    private final RestTemplate restTemplate = new RestTemplate();

    public ExecuteResponse execute(String sql, Object[] params) throws SQLException {
        try {
            String url = "http://localhost:8080/execute";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            ExecuteRequest request = new ExecuteRequest(sql, params);
            HttpEntity<ExecuteRequest> entity = new HttpEntity<>(request, headers);

            ResponseEntity<ExecuteResponse> response = restTemplate.exchange(
                    url,
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
