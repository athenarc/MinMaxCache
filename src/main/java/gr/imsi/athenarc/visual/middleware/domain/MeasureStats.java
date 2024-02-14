package gr.imsi.athenarc.visual.middleware.domain;

public class MeasureStats {
    private final double mean;
    private final double min;
    private final double max;

    public MeasureStats(double mean, double min, double max) {
        this.mean = mean;
        this.min = min;
        this.max = max;
    }

    public double getMean() {
        return mean;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }
}
