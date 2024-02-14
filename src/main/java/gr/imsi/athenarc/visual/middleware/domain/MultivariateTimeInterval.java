package gr.imsi.athenarc.visual.middleware.domain;

import java.util.List;

public class MultivariateTimeInterval implements TimeInterval{

    TimeInterval timeInterval;
    List<Integer> measures;

    public MultivariateTimeInterval(TimeInterval timeInterval, List<Integer> measures) {
        this.timeInterval = timeInterval;
        this.measures = measures;
    }

    public TimeInterval getInterval() {
        return timeInterval;
    }

    public List<Integer> getMeasures(){
        return measures;
    }
    @Override
    public long getFrom() {
        return 0;
    }

    @Override
    public long getTo() {
        return 0;
    }
}
