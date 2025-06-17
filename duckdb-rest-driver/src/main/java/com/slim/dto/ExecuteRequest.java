package com.slim.dto;

public class ExecuteRequest {
    private String sql;
    private Object[] params;

    public ExecuteRequest() {
        // Constructeur par défaut pour la désérialisation
    }

    public ExecuteRequest(String sql, Object[] params) {
        this.sql = sql;
        this.params = params;
    }

    public String getSql() {
        return sql;
    }
    public void setSql(String sql) {
        this.sql = sql; 
    }


    public Object[] getParams() {
        return params;
    }
    
    public void setParams(Object[] params) {
        this.params = params;
    }

}
