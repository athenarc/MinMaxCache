package gr.imsi.athenarc.visual.middleware.domain;

public class TableInfo {
    String table;
    String schema;
    
    public TableInfo(String table) {
        this.table = table;
    }
    public TableInfo(String table, String schema) {
        this.table = table;
        this.schema = schema;
    }
    public TableInfo() {
    }
    public String getTable() {
        return table;
    }
    public void setTable(String table) {
        this.table = table;
    }
    public String getSchema() {
        return schema;
    }
    public void setSchema(String schema) {
        this.schema = schema;
    }
    
}
