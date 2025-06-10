package ord.duckdb.dto;

import java.util.List;


public class ExecuteResponse {
    private ExecuteDuckDBResultSetMetaData metadata;
    private List<List<Object>> data;
    private boolean error = false;
    private String errorMessage = null;

    public ExecuteResponse(ExecuteDuckDBResultSetMetaData metadata, List<List<Object>> data) {
        this.metadata = metadata;
        this.data = data;
    }

    public ExecuteResponse(String errorMessage) {
        this.error= true;
        this.errorMessage = errorMessage;
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

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isError() {
        return error;
    }


}
