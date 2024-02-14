package gr.imsi.athenarc.visual.middleware.domain;


/**
 * Represents a data point that aggregates a series of raw, non-aggregated data points, along with their aggregated measure values
 */
public interface AggregatedDataPoint extends DataPoint, TimeInterval {

    /**
     * @return The number of raw data points represented by this aggregated data point.
     */
    int getCount();

    /**
     * @return The aggregate measure metadata of the raw data points
     * represented by this aggregated data point.
     */
    Stats getStats();

    int getMeasure();

    default String getString() {
        return "{from: " + getFrom() + ", to: " + getTo() + ", stats: " + getStats() + "}";
    }

}
