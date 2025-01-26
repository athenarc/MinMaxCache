package gr.imsi.athenarc.visual.middleware.web.rest.model;

import gr.imsi.athenarc.visual.middleware.domain.QueryResults;

public class QueryDTO {
    
    public static class QueryRequest {
        public VisualQuery query;


        public QueryRequest() {
        }

        public QueryRequest(VisualQuery query, String schema, String table) {
            this.query = query;
        }
    }


    public static class QueryResponse {
        public String message;
        public QueryResults queryResults;
       
        public QueryResponse() {

        }

        public QueryResponse(String message, QueryResults queryResults){
            this.message = message;
            this.queryResults = queryResults;
        }
    }
}
