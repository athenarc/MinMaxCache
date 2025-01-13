package gr.imsi.athenarc.visual.middleware.domain;

public class ViewPort {

    private int width;
    private int height;
    public ViewPort() {}

    public ViewPort(int width, int height) {
        this.width = width;
        this.height = height;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    /**
     * Returns the vertical pixel id of the given value for the specified measure
     * considering the current min and max values of the measure over the entire view port.
     *
     * @param value
     * @param viewPortStats
     * @return the vertical pixel id of the given value for the specified measure
     */
    public int getPixelId(double value, Stats viewPortStats) {
        return (int) ((double) (this.getHeight() - 1) * (value - viewPortStats.getMinValue()) / (viewPortStats.getMaxValue() - viewPortStats.getMinValue()));
    }

    @Override
    public String toString() {
        return "{" +
                "width=" + width +
                ", height=" + height +
                '}';
    }
}
