package com.slim.dto;

import java.util.List;


public class ExecuteResponse {
    public ExecuteDuckDBResultSetMetaData metadata;
    public List<List<Object>> data;

    public ExecuteResponse(ExecuteDuckDBResultSetMetaData metadata, List<List<Object>> data) {
        this.metadata = metadata;
        this.data = data;
    }

    public ExecuteResponse() {
        // Default constructor for deserialization
    }   
    public ExecuteDuckDBResultSetMetaData getMetadata() {
        return metadata;
    }


    public void setMetadata(ExecuteDuckDBResultSetMetaData metadata) {
        this.metadata = metadata;
    }

    public List<List<Object>> getData() {
        return data;
    }


    public void setData(List<List<Object>> data) {
        this.data = data;
    }


}

