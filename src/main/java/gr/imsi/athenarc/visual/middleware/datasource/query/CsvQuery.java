package gr.imsi.athenarc.visual.middleware.datasource.query;

import java.util.List;
import java.util.Map;

import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

/**
 * Represents a time series data source query
 */
public class CsvQuery extends DataSourceQuery {

    
    public CsvQuery(long from, long to, Map<String, List<TimeInterval>> missingIntervalsPerMeasure, Map<String, Integer> numberOfGroups) {
        super(from, to, missingIntervalsPerMeasure, numberOfGroups);
    }

    public CsvQuery(long from, long to, Map<String, List<TimeInterval>> missingIntervalsPerMeasure) {
        super(from, to, missingIntervalsPerMeasure);
    }

    @Override
    public String m4QuerySkeleton() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("No such method for CSV files");
    }

    @Override
    public String minMaxQuerySkeleton() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("No such method for CSV files");
    }

    @Override
    public String rawQuerySkeleton() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("No such method for CSV files");
    }

}


