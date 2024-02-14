package gr.imsi.athenarc.visual.middleware.domain.Query;

import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.ViewPort;
import gr.imsi.athenarc.visual.middleware.experiments.util.UserOpType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

public class Query implements TimeInterval {

    long from;
    long to;
    List<Integer> measures;
    ViewPort viewPort;
    QueryMethod queryMethod;
    UserOpType opType;
    private HashMap<Integer, Double[]> filter;
    float accuracy;

    public Query() {}

    public Query(long from, long to, float accuracy, HashMap<Integer, Double[]> filter,
                 QueryMethod queryMethod, List<Integer> measures, ViewPort viewPort, UserOpType opType) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.filter = filter;
        this.viewPort = viewPort;
        this.queryMethod = queryMethod;
        this.opType = opType;
        this.accuracy = accuracy;
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    @Override
    public String getFromDate() {
        return getFromDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getFromDate(String format) {
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return getToDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public ViewPort getViewPort() {
        return viewPort;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public UserOpType getOpType() {
        return opType;
    }

    public QueryMethod getQueryMethod() {
        return queryMethod;
    }

    public float getAccuracy() {
        return accuracy;
    }

    public void setQueryMethod(QueryMethod queryMethod) {
        this.queryMethod = queryMethod;
    }

    public void setOpType(UserOpType opType) {
        this.opType = opType;
    }

    public HashMap<Integer, Double[]> getFilter() {
        return filter;
    }

    @Override
    public String toString() {
        return "Query{" +
                "from=" + from +
                ", to=" + to +
                ", measures=" + measures +
                ", viewPort=" + viewPort +
                ", queryMethod=" + queryMethod +
                ", filter= " + filter +
                ", opType=" + opType +
                ", accuracy=" + accuracy +
                '}';
    }
}
