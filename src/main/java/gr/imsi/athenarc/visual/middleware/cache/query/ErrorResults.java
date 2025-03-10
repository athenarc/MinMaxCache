package gr.imsi.athenarc.visual.middleware.cache.query;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

public class ErrorResults {
    
    private double error;
    private List<RangeSet<Integer>> missingPixels;
    private List<RangeSet<Integer>> falsePixels;
    
    public ErrorResults() {
        this.error = 0.0;
        missingPixels = new ArrayList<>();
        falsePixels = new ArrayList<>();
    }

    public double getError() {
        return error;
    }
    public void setError(double error) {
        this.error = error;
    }

    public List<RangeSet<Integer>> getMissingPixels() {
        return missingPixels;
    }

    public void setMissingPixels(List<RangeSet<Integer>> missingPixels) {
        this.missingPixels = missingPixels;
    }


    public List<RangeSet<Integer>> getFalsePixels() {
        return falsePixels;
    }

    public void setFalsePixels(List<RangeSet<Integer>> falsePixels) {
        this.falsePixels = falsePixels;
    }

    public List<List<String>> getMissingPixelsAsString() {
        return missingPixels.stream()
                .map(rangeSet -> rangeSet.asRanges().stream()
                        .map(Range::toString)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    public List<List<String>> getFalsePixelsAsString() {
        return falsePixels.stream()
                .map(rangeSet -> rangeSet.asRanges().stream()
                        .map(Range::toString)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
    }


    @Override
    public String toString() {
        return "ErrorResults [error=" + error + ", missingPixels=" + missingPixels + ", falsePixels=" + falsePixels
                + "]";
    }
    
}
