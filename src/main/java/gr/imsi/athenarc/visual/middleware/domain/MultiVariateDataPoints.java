package gr.imsi.athenarc.visual.middleware.domain;

public interface MultiVariateDataPoints extends Iterable<MultiVariateDataPoint>, TimeInterval  {

    /**
     * The measures in order of the values of the underlying MultiVariateDataPoint instances
     */
    int[] getMeasures();
}
