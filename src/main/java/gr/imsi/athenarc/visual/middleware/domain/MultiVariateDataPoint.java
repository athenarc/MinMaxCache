package gr.imsi.athenarc.visual.middleware.domain;

public interface MultiVariateDataPoint {
    /**
     * Returns the timestamp(epoch time in milliseconds) of this data point.
     */
    long getTimestamp();

    /**
     * Returns a single measure value for the {@code timestamp)
     */
    double[] getValues();
}
