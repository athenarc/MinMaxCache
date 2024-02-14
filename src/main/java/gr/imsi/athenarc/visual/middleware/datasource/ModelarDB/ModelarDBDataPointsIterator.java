package gr.imsi.athenarc.visual.middleware.datasource.ModelarDB;

import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import cfjd.org.apache.arrow.flight.FlightStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class ModelarDBDataPointsIterator implements Iterator<DataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

//    private final FlightStream flightStream;
//    private final List<List<Integer>> measures;
//    private final String timeCol;
//    private final String valueCol;
//    private final String idCol;
//    private Iterator<Row> iterator;

    public ModelarDBDataPointsIterator(List<Integer> measures,
                                       String timeCol,
                                       String valueCol,
                                       String idCol,
                                       FlightStream flightStream){
//        this.measures = measures;
//        this.flightStream = flightStream;
//        this.idCol = idCol;
//        this.timeCol = timeCol;
//        this.valueCol = valueCol;
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
    public ImmutableDataPoint next() {
//        double[] values = new double[measures.size()];
//        long timestamp = 0L;
//        int i = 0;
//        while (i < measures.size() && hasNext()) {
//            Row row = iterator.next();
//            timestamp = row.getTimeStampMilli(timeCol);
//            double val = row.getFloat4(valueCol);
//            values[i] = val;
//            i ++;
//        }
//        return new ImmutableDataPoint(timestamp, values);
        return null;
    }
}
