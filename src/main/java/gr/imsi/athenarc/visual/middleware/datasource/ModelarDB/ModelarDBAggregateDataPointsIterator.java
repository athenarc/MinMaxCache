package gr.imsi.athenarc.visual.middleware.datasource.ModelarDB;

import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import cfjd.org.apache.arrow.flight.FlightStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class ModelarDBAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);
//
//    private final FlightStream flightStream;
//    private final List<List<Integer>> measures;
//    private final long from;
//    private final long to;
//
//    private final long aggregateInterval;
//    private final int noOfGroups;
//
//    private Iterator<Row> iterator;
//
//    private final String timeCol;
//    private final String valueCol;
//    private final String idCol;


    public ModelarDBAggregateDataPointsIterator(long from, long to, List<Integer> measures,
                                                String timeCol,
                                                String valueCol,
                                                String idCol,
                                                FlightStream flightStream, int[] noOfGroups){
//        this.measures = measures;
//        this.flightStream = flightStream;
//        this.aggregateInterval = (to - from) / noOfGroups;
//        this.from = from;
//        this.to = to;
//        this.idCol = idCol;
//        this.timeCol = timeCol;
//        this.valueCol = valueCol;
//        this.noOfGroups = noOfGroups;
//        flightStream.next();
//        Table t = new Table(flightStream.getRoot());
//        this.iterator  = t.iterator();
    }

    @Override
    public boolean hasNext() {
//        if(iterator.hasNext()) return true;
//        else {
//            if(flightStream.next()){
//                Table t = new Table(flightStream.getRoot());
//                this.iterator  = t.iterator();
//                return true;
//            }
//            else return false;
//        }
        return false;
    }

    @Override
    public AggregatedDataPoint next() {
        return null;
    }

//
//    @Override
//    public AggregatedDataPoint next() {
//        NonTimestampedStatsAggregator statsAggregator = new NonTimestampedStatsAggregator(measures);
//        long firstTimestamp = from;
//        int k = 0;
//        int i = 0;
//        while(i < measures.size() && hasNext()){
//            int m = measures.get(i);
//            Row row = iterator.next();
//            String sensor = new String(row.getVarChar(idCol), StandardCharsets.UTF_8);
//            k = (int) row.getFloat8("k");
//            double v_min = row.getFloat4("v_min");
//            double v_max = row.getFloat4("v_max");
//            statsAggregator.accept(v_min, m);
//            statsAggregator.accept(v_max, m);
////            LOG.debug("{} - {}", m, sensor);
//
//            i ++;
//        }
//        firstTimestamp = from + k * aggregateInterval;
//        long lastTimestamp = (k != noOfGroups - 1) ? from + (k + 1) * aggregateInterval : to;
//        statsAggregator.setFrom(firstTimestamp);
//        statsAggregator.setTo(lastTimestamp);
////        LOG.debug("{}", measures.stream()
////                .map(m -> m + " - " + statsAggregator.getMinValue(m)).collect(Collectors.toList()));
//        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
//    }

}
