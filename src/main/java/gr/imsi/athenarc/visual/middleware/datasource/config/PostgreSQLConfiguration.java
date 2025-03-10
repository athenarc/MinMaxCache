package gr.imsi.athenarc.visual.middleware.datasource.config;

public class PostgreSQLConfiguration implements DataSourceConfiguration {
    private final String url;
    private final String username;
    private final String password;
    private final String schema;
    private final String table;
    private final String timeFormat;

    private PostgreSQLConfiguration(Builder builder) {
        this.url = builder.url;
        this.username = builder.username;
        this.password = builder.password;
        this.schema = builder.schema;
        this.table = builder.table;
        this.timeFormat = builder.timeFormat;
    }

    public String getUrl() { return url; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getSchema() { return schema; }
    public String getTable() { return table; }
    public String getTimeFormat() { return timeFormat; }

    public static class Builder {
        private String url, username, password, schema, table, timeFormat;

        public Builder url(String url) { this.url = url; return this; }
        public Builder username(String username) { this.username = username; return this; }
        public Builder password(String password) { this.password = password; return this; }
        public Builder schema(String schema) { this.schema = schema; return this; }
        public Builder table(String table) { this.table = table; return this; }
        public Builder timeFormat(String timeFormat) { this.timeFormat = timeFormat; return this; }

        public PostgreSQLConfiguration build() {
            if (url == null || username == null || password == null || 
                schema == null || table == null || timeFormat == null) {
            throw new IllegalStateException("All configuration values must be provided");
            }  
            return new PostgreSQLConfiguration(this);
        }
    }
}
