package gr.imsi.athenarc.visual.middleware.domain;

import gr.imsi.athenarc.visual.middleware.cache.TimeSeriesSpan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TTIQueryResults {
    private List<TimeSeriesSpan> overlappingSpans;
    private List<TimeRange> missingIntervals;
    private long from;
    private long to;

    public TTIQueryResults(long from, long to, List<TimeSeriesSpan> overlappingIntervals, List<TimeRange> missingIntervals) {
        this.from = from;
        this.to = to;
        this.overlappingSpans = overlappingIntervals;
        this.missingIntervals = missingIntervals;
        overlappingIntervals.sort((i1, i2) -> (int) (i1.getFrom() - i2.getFrom())); // Sort intervals
    }

    public List<TimeSeriesSpan> getOverlappingSpans() {
        return overlappingSpans;
    }

    public List<TimeRange> getMissingIntervals() {
        return missingIntervals;
    }

    public boolean addAll(List<TimeSeriesSpan> timeSeriesSpans){
        boolean added = overlappingSpans.addAll(timeSeriesSpans);
        if(!added) return false;
        overlappingSpans.sort(Comparator.comparing(TimeSeriesSpan::getFrom)); // Sort intervals
        missingIntervals = new ArrayList<>();
        return true;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }
}
