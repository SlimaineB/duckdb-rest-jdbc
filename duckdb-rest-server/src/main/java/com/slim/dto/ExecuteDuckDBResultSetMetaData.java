package com.slim.dto;

public class ExecuteDuckDBResultSetMetaData {

    private  int param_count;
    private  int column_count;
    private  String[] column_names;
    private  String[] column_types_string;
    private  String return_type;
    private  String[] column_types_details;
    private  String[] param_types_details;
    private  String[] param_types_string;

    public ExecuteDuckDBResultSetMetaData() {
        // Default constructor for deserialization
    }

    public ExecuteDuckDBResultSetMetaData(int param_count, int column_count, String[] column_names,
                                   String[] column_types_string, String[] column_types_details, String return_type,
                                   String[] param_types_string, String[] param_types_details) {
        this.param_count = param_count;
        this.column_count = column_count;
        this.column_names = column_names;
        this.column_types_string = column_types_string;
        this.column_types_details = column_types_details;
        this.return_type = return_type;
        this.param_types_string = param_types_string;
        this.param_types_details = param_types_details;
    }

    public int getParam_count() {
        return param_count;
    }

    public int getColumn_count() {
        return column_count;
    }

    public String[] getColumn_names() {
        return column_names;
    }

    public String[] getColumn_types_string() {
        return column_types_string;
    }

    public String getReturn_type() {
        return return_type;
    }

    public String[] getColumn_types_details() {
        return column_types_details;
    }

    public String[] getParam_types_details() {
        return param_types_details;
    }

    public String[] getParam_types_string() {
        return param_types_string;
    }
}
