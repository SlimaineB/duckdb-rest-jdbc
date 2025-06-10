package org.duckdb.dto;

public class ExecuteRequest {
    public String sql;
        public Object[] params;

    public ExecuteRequest(String sql, Object[] params) {
        this.sql = sql;
        this.params = params;
    }
}
