package gr.imsi.athenarc.visual.middleware.domain;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that computes the maximum number of pixel errors.
 */
public class MaxErrorEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(MaxErrorEvaluator.class);

    private final ViewPort viewPort;

    private final List<PixelColumn> pixelColumns;

    private List<TimeInterval> missingRanges;


    private List<RangeSet<Integer>> missingPixels;
    private List<RangeSet<Integer>> falsePixels;

    public MaxErrorEvaluator(ViewPort viewPort, List<PixelColumn> pixelColumns) {
        this.viewPort = viewPort;
        this.pixelColumns = pixelColumns;
        this.missingPixels = new ArrayList<>();
        this.falsePixels = new ArrayList<>();
    }

    public List<Double> computeMaxPixelErrorsPerColumn() {
        List<Double> maxPixelErrorsPerColumn = new ArrayList<>();
        missingRanges = new ArrayList<>();

        // The stats aggregator for the whole query interval to keep track of the min/max values
        // and determine the y-axis scale.
        StatsAggregator viewPortStatsAggregator = new StatsAggregator();
        pixelColumns.forEach(pixelColumn -> viewPortStatsAggregator.combine(pixelColumn.getStats()));
        LOG.debug("Viewport stats: {}", viewPortStatsAggregator);

        for (int i = 0; i < pixelColumns.size(); i++) {
            PixelColumn currentPixelColumn = pixelColumns.get(i);
            Range<Integer> maxInnerColumnPixelRanges = currentPixelColumn.computeMaxInnerPixelRange(viewPortStatsAggregator);
            RangeSet<Integer> pixelColumnFalsePixels = TreeRangeSet.create();
            RangeSet<Integer> pixelColumnMissingPixels = TreeRangeSet.create();


            if (maxInnerColumnPixelRanges == null) {
                maxPixelErrorsPerColumn.add(null);
                missingRanges.add(currentPixelColumn.getRange()); // add range as missing
                continue;
            }
            
            // Check if there is a previous PixelColumn
            if (i > 0) {
                PixelColumn previousPixelColumn = pixelColumns.get(i - 1);
                if (previousPixelColumn.getStats().getCount() != 0) {
                    Range<Integer> leftMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(previousPixelColumn.getStats().getLastTimestamp(), previousPixelColumn.getStats().getLastValue(),
                            currentPixelColumn.getStats().getFirstTimestamp(), currentPixelColumn.getStats().getFirstValue(), viewPortStatsAggregator);
                            pixelColumnFalsePixels.add(leftMaxFalsePixels);

                    if (!getMaxMissingInterColumnPixels(previousPixelColumn, currentPixelColumn, pixelColumnMissingPixels, viewPortStatsAggregator)){
                        maxPixelErrorsPerColumn.add(null);
                        missingRanges.add(currentPixelColumn.getRange()); // add range as missing
                        continue;
                    }
                }
            }
            // Check if there is a next PixelColumn
            if (i < pixelColumns.size() - 1) {
                PixelColumn nextPixelColumn = pixelColumns.get(i + 1);
                if (nextPixelColumn.getStats().getCount() != 0) {
                    Range<Integer> rightMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(currentPixelColumn.getStats().getLastTimestamp(), currentPixelColumn.getStats().getLastValue(),
                            nextPixelColumn.getStats().getFirstTimestamp(), nextPixelColumn.getStats().getFirstValue(), viewPortStatsAggregator);
                            pixelColumnFalsePixels.add(rightMaxFalsePixels);

                    if (!getMaxMissingInterColumnPixels(currentPixelColumn, nextPixelColumn, pixelColumnMissingPixels, viewPortStatsAggregator)){
                        maxPixelErrorsPerColumn.add(null);
                        missingRanges.add(currentPixelColumn.getRange()); // add range as missing
                        continue;
                    }
                }
            }

            // Remove actual inner column pixels 
            pixelColumnFalsePixels.remove(currentPixelColumn.getActualInnerColumnPixelRange(viewPortStatsAggregator));
            pixelColumnMissingPixels.remove(currentPixelColumn.getActualInnerColumnPixelRange(viewPortStatsAggregator));

            RangeSet<Integer> pixelColumnErrorPixels = TreeRangeSet.create();
            pixelColumnErrorPixels.addAll(pixelColumnFalsePixels);
            pixelColumnErrorPixels.addAll(pixelColumnMissingPixels);
            // Calculate pixel errors
            int maxWrongPixels = pixelColumnErrorPixels.asRanges().stream()
                    .mapToInt(range -> range.upperEndpoint() - range.lowerEndpoint() + 1)
                    .sum();

            // Normalize the result
            maxPixelErrorsPerColumn.add(((double) maxWrongPixels / viewPort.getHeight()));

            // Add sets to list for return on queryResults
            missingPixels.add(pixelColumnMissingPixels);
            falsePixels.add(pixelColumnFalsePixels);
        }
        LOG.debug("{}", maxPixelErrorsPerColumn);
        return maxPixelErrorsPerColumn;
    }

    /**
     * Computes the maximum missing inter-column pixel range for these adjacent pixel columns.
     * For this we consider only the case of fully-contained groups at the left and right boundary of the intersection.
     * In case of partial containment, this potential missing pixels are is already accounted for in the potential inner-column pixel errors.
     * This method must be called after the computation of the inner-column pixel errors.
     *
     * @param leftPixelColumn
     * @param rightPixelColumn
     * @return the maximum missing column pixel range or null in case of partially-contained group at the intersection between the two columns.
     */
    private boolean getMaxMissingInterColumnPixels(PixelColumn leftPixelColumn, PixelColumn rightPixelColumn,
                                                   RangeSet<Integer> pixelColumnMissingPixels,
                                                   StatsAggregator viewPortStatsAggregator) {
        // check if there is a partially-contained group at the intersection between the two columns
        AggregatedDataPoint leftPartial = leftPixelColumn.getLeftPartial();
        if (leftPartial == null) {
            if (rightPixelColumn.getLeft().size() > 0 && leftPixelColumn.getRight().size() > 0) {
                // case of fully-contained groups at the boundary of the intersection
                AggregatedDataPoint left = rightPixelColumn.getLeft().get(0);
                AggregatedDataPoint right = leftPixelColumn.getRight().get(0);
                int leftMinPixelId = viewPort.getPixelId(left.getStats().getMinValue(), viewPortStatsAggregator);
                int rightMaxPixelId = viewPort.getPixelId(right.getStats().getMaxValue(), viewPortStatsAggregator);
                int leftMaxPixelId = viewPort.getPixelId(left.getStats().getMaxValue(), viewPortStatsAggregator);
                int rightMinPixelId = viewPort.getPixelId(right.getStats().getMinValue(), viewPortStatsAggregator);

                pixelColumnMissingPixels.add(Range.closed(Math.min(leftMinPixelId, rightMaxPixelId), Math.max(leftMinPixelId, rightMaxPixelId)));
                pixelColumnMissingPixels.add(Range.closed(Math.min(leftMaxPixelId, rightMinPixelId), Math.max(leftMaxPixelId, rightMinPixelId)));

            } else {
                return false;
            }
        } else {
            pixelColumnMissingPixels.add(Range.closed(viewPort.getPixelId(leftPartial.getStats().getMinValue(), viewPortStatsAggregator),
                    viewPort.getPixelId(leftPartial.getStats().getMaxValue(), viewPortStatsAggregator)));
        }
        return true;
    }


    public List<TimeInterval> getMissingRanges() {
        return missingRanges;
    }

    public List<RangeSet<Integer>> getMissingPixels() {
        return missingPixels;
    }

    public List<RangeSet<Integer>> getFalsePixels() {
        return falsePixels;
    }


}

