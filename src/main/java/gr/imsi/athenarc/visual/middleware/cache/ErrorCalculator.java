package gr.imsi.athenarc.visual.middleware.cache;

import gr.imsi.athenarc.visual.middleware.domain.*;
import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.RangeSet;

import java.util.*;

public class ErrorCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private MaxErrorEvaluator maxErrorEvaluator;
    private boolean hasError = true;
    private long pixelColumnInterval;
    private double error;

    public double calculateTotalError(List<PixelColumn> pixelColumns, ViewPort viewPort, long pixelColumnInterval, double accuracy) {
        // Calculate errors using processed data
        maxErrorEvaluator = new MaxErrorEvaluator(viewPort, pixelColumns);
        this.pixelColumnInterval = pixelColumnInterval;
        List<Double> pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumn();

        // Find the part of the query interval that is not covered by the spans in the interval tree.
        int validColumns = 0;
        error = 0.0;
        for (Double pixelColumnError : pixelColumnErrors) {
            if(pixelColumnError != null) {
                validColumns += 1;
                error += pixelColumnError;
            }
        }
        LOG.debug("Valid columns: {}", validColumns);
        error /= validColumns;
        hasError = error > 1 - accuracy;
        return error;
    }

    public List<TimeInterval> getMissingIntervals() {
        List<TimeInterval> missingIntervals = maxErrorEvaluator.getMissingRanges();
        missingIntervals = DateTimeUtil.groupIntervals(pixelColumnInterval, missingIntervals);
        LOG.info("Unable to Determine Errors: " + missingIntervals);
        return missingIntervals;
    }

    public List<RangeSet<Integer>> getMissingPixels() {
        return maxErrorEvaluator.getMissingPixels();
    }

    public List<RangeSet<Integer>> getFalsePixels() {
        return maxErrorEvaluator.getFalsePixels();
    }

    public boolean hasError(){
        return hasError;
    }
}
