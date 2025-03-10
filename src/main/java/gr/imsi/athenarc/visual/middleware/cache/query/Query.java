package gr.imsi.athenarc.visual.middleware.cache.query;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.ViewPort;

public class Query implements TimeInterval {

    private final long from;
    private final long to;
    private final List<Integer> measures;
    private final ViewPort viewPort;
    private final double accuracy;

    public Query(long from, long to, List<Integer> measures, int width, int height, double accuracy) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.accuracy = accuracy;
        this.viewPort = new ViewPort(width, height);
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

    public double getAccuracy(){
        return accuracy;
    }

    @Override
    public String toString() {
        return "Query{" +
                "from=" + from +
                ", to=" + to +
                ", measures=" + measures +
                ", viewPort=" + viewPort +
                '}';
    }
}
