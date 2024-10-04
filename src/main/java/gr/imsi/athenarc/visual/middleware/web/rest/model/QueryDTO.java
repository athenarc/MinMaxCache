package gr.imsi.athenarc.visual.middleware.web.rest.model;

import gr.imsi.athenarc.visual.middleware.domain.Query.Query;

public class QueryDTO {
    
    public static class QueryRequest {
        public Query query;
        public String schema;
        public String table;


        public QueryRequest() {
        }

        public QueryRequest(Query query, String schema, String table) {
            this.query = query;
            this.schema = schema;
            this.table = table;
        }
    }


    public static class QueryResponse {
        
    }
}
