package gr.imsi.athenarc.visual.middleware.web.rest.model;

import gr.imsi.athenarc.visual.middleware.methods.VisualQuery;
import gr.imsi.athenarc.visual.middleware.methods.VisualQueryResults;

public class QueryDTO {
    
    public static class QueryRequest {
        public VisualQuery query;


        public QueryRequest() {
        }

        public QueryRequest(VisualQuery query) {
            this.query = query;
        }
    }

    public static class QueryResponse {
        public String message;
        public VisualQueryResults queryResults;
       
        public QueryResponse() {

        }

        public QueryResponse(String message, VisualQueryResults queryResults){
            this.message = message;
            this.queryResults = queryResults;
        }
    }
}
