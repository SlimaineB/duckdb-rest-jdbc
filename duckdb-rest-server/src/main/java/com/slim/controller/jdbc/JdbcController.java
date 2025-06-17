package com.slim.controller.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.slim.dto.ExecuteRequest;
import com.slim.dto.ExecuteResponse;
import com.slim.service.QueryService;


/**
 * Contrôleur pour exécuter des requêtes SQL via JDBC.
 * Utilisé pour les requêtes envoyées par l'interface utilisateur.
 */
@RestController
@RequestMapping("/jdbc")
public class JdbcController {

    private static final Logger logger = LoggerFactory.getLogger(JdbcController.class);

    private final QueryService queryService;

    @Autowired
    public JdbcController(QueryService queryService) {
        this.queryService = queryService;
    }

    @PostMapping("/execute")
    public ResponseEntity<ExecuteResponse> execute(@RequestBody ExecuteRequest request) {
        ExecuteResponse response = queryService.execute(request);
        if (response.isError()) {
            logger.error("Erreur lors de l'exécution de la requête SQL: {}", response.getErrorMessage());
            return ResponseEntity.status(500).body(response);
        }
        logger.info("Requête SQL exécutée avec succès: {}", request.getSql());
        return ResponseEntity.ok(response);
    }
}
