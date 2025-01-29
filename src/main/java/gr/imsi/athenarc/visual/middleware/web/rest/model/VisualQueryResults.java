package gr.imsi.athenarc.visual.middleware.web.rest.model;

import java.util.List;
import java.util.Map;

import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

public class VisualQueryResults {

    Map<Integer, List<DataPoint>>  data;
    TimeInterval timeRange;
    double queryTime;
    
    Map<String, String> metrics;

    public void setTimeRange(TimeInterval timeRange) {
        this.timeRange = timeRange;
    }

    public void setData(Map<Integer, List<DataPoint>> data) {
        this.data = data;
    }

    public void setMetrics(Map<String, String> metrics) {
        this.metrics = metrics;
    }

    public void setQueryTime(double queryTime) {
        this.queryTime = queryTime;
    }

    public double getQueryTime() {
        return queryTime;
    }

    public TimeInterval getTimeRange() {
        return timeRange;
    }   

    public Map<Integer, List<DataPoint>> getData() {
        return data;
    }

    public Map<String, String> getMetrics() {
        return metrics;
    }

    public String toString() {
        return "VisualQueryResults{" +
            "data=" + data +
            ", timeRange=" + timeRange +
            ", queryTime=" + queryTime +
            ", metrics=" + metrics +
            '}';
    }
}
