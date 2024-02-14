package gr.imsi.athenarc.visual.middleware.domain;

import java.time.ZonedDateTime;

public interface PixelAggregatedDataPoint extends AggregatedDataPoint  {

    public ZonedDateTime getSubPixel();
    public ZonedDateTime getPixel();
    public ZonedDateTime getNextPixel();
    public AggregateInterval getInterval();
    public PixelAggregatedDataPoint persist();
    public boolean startsBefore(ZonedDateTime zonedDateTime);
    public boolean endsBefore(ZonedDateTime zonedDateTime);
    public boolean endsAfter(ZonedDateTime zonedDateTime);

    public Stats getStats();


}
