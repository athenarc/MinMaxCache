package gr.imsi.athenarc.visual.middleware.datasource;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.visual.middleware.domain.DataPoints;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

/**
 * Represents a time series data source
 */
public interface DataSource {

    /**
     * Returns a {@link DataPoints} instance to access the data points in the time series, that
     * have a timestamp greater than or equal to the startTimestamp,
     * and less than or equal to the endTimestamp.
     *
     * @param from The start time of range to fetch
     * @param to The end time of range to fetch
     * @param measures       The measure values to include in every data point
     */
    public DataPoints getDataPoints(long from, long to, List<Integer> measures);

    public DataPoints getDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure);
    /**
     * Returns a {@link DataPoints} instance to access all the data points in the time series.
     *
     * @param measures The measure values to include in every data point
     */
    public DataPoints getAllDataPoints(List<Integer> measures);

    /**
     * For visualization
     */

    /**
     * Returns an {@link AggregatedDataPoints} instance to access the first,last,min,max
     * data points in the time series, that have a timestamp between each of the missing intervals of each measure
     * @param from The start time of range to fetch
     * @param to The end time of range to fetch
     * @param missingIntervalsPerMeasure The sub-ranges missing for each measure
     * @param numberOfGroups The number of groups needed to be fetched for each measure
     */
    AggregatedDataPoints getM4DataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure, Map<Integer, Integer> numberOfGroups);


    /**
     * Returns an {@link AggregatedDataPoints} instance to access the min,max data points in the time series, that
     * have a timestamp between each of the missing intervals of each measure
     * @param from The start time of range to fetch
     * @param to The end time of range to fetch
     * @param missingIntervalsPerMeasure The sub-ranges missing for each measure
     * @param numberOfGroups The number of groups needed to be fetched for each measure
     */
    AggregatedDataPoints getMinMaxDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure, Map<Integer, Integer> numberOfGroups);

    public AbstractDataset getDataset();

    public void closeConnection();

}
