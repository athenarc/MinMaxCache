package gr.imsi.athenarc.visual.middleware.web.rest.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VisualQuery  {

    Algorithm algorithm;

    long from;
    long to;
    List<Integer> measures;
    int width;
    int height;

    public String schema;
    public String table;
    
    private Map<String, String> params = new HashMap<>();


    public VisualQuery() {}

    public VisualQuery(Algorithm algorithm, long from, long to, List<Integer> measures, int width, int height, String schema, String table, Map<String, String> params) {    
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.width = width;
        this.height = height;
        this.schema = schema;
        this.table = table;
        this.params = params;
        this.algorithm = algorithm;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public Map<String, String> getParams() {
        return params;
    }
    
    @Override
    public String toString() {
        return "Query{" +
                "from=" + from +
                ", to=" + to +
                ", measures=" + measures +
                ", width=" + width +
                ", height=" + height +
                ", algorithm=" + algorithm +
                ", schema=" + schema +
                ", table=" + table +
                ", params=" + params +
                '}';
    }
}
