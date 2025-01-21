package gr.imsi.athenarc.visual.middleware.cache;

import gr.imsi.athenarc.visual.middleware.domain.*;
import gr.imsi.athenarc.visual.middleware.domain.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.query.Query;
import gr.imsi.athenarc.visual.middleware.domain.query.QueryMethod;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrefetchManager {

    private final AbstractDataset dataset;
    private final DataProcessor dataProcessor;
    private final CacheManager cacheManager;
    private final double prefetchingFactor;

    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    public PrefetchManager(AbstractDataset dataset, double prefetchingFactor,
                           CacheManager cacheManager, DataProcessor dataProcessor) {
        this.prefetchingFactor = prefetchingFactor;
        this.cacheManager = cacheManager;
        this.dataProcessor = dataProcessor;
        this.dataset = dataset;
    }

    long[] extendInterval(long from, long to, double factor){
        long interval = to - from;
        long difference = (long) (interval * (factor / 2));
        long newFrom = Math.max(dataset.getTimeRange().getFrom(), from - difference);
        long newTo = Math.min(dataset.getTimeRange().getTo(), to + difference);

        return new long[]{newFrom, newTo};
    }

    public void prefetch(Query query, Map<Integer, Integer> aggFactors){
        if(prefetchingFactor == 0) return;
        // Setup prefetching range
        long[] prefetchingInterval = extendInterval(query.getFrom(), query.getTo(), prefetchingFactor);
        long prefetchingFrom = prefetchingInterval[0];
        long prefetchingTo = prefetchingInterval[1];

        // Initialize prefetch query
        // Then fetch data similarly to CacheQueryExecutor
        Query prefetchQuery = new Query(prefetchingFrom, prefetchingTo, query.getAccuracy(), query.getFilter(),
                query.getQueryMethod(), query.getMeasures(), query.getViewPort(), query.getOpType());
        List<Integer> measures = prefetchQuery.getMeasures();
        ViewPort viewPort = prefetchQuery.getViewPort();
        long from = prefetchQuery.getFrom();
        long to = prefetchQuery.getTo();
        long pixelColumnInterval = (to - from) / viewPort.getWidth();

        QueryMethod queryMethod = QueryMethod.MIN_MAX;
        Map<Integer, List<TimeSeriesSpan>> overlappingSpansPerMeasure = cacheManager.getFromCache(prefetchQuery, pixelColumnInterval);
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>(measures.size());
        // Initialize Pixel Columns
        Map<Integer, List<PixelColumn>> pixelColumnsPerMeasure = new HashMap<>(measures.size()); // Lists of pixel columns. One list for every measure.
        for (int measure : measures) {
            List<PixelColumn> pixelColumns = new ArrayList<>();
            for (long j = 0; j < viewPort.getWidth(); j++) {
                long pixelFrom = from + (j * pixelColumnInterval);
                long pixelTo = pixelFrom + pixelColumnInterval;
                PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, viewPort);
                pixelColumns.add(pixelColumn);
            }
            pixelColumnsPerMeasure.put(measure, pixelColumns);
        }

        for(int measure : measures) {
            // Get overlapping spans
            List<TimeSeriesSpan> overlappingSpans = overlappingSpansPerMeasure.get(measure);

            // Add to pixel columns
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measure);
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, overlappingSpans);

            // Calculate Error
            ErrorCalculator errorCalculator = new ErrorCalculator();
            errorCalculator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, prefetchQuery.getAccuracy());
            List<TimeInterval> missingIntervalsForMeasure = errorCalculator.getMissingIntervals();
            if(!missingIntervalsForMeasure.isEmpty())
                missingIntervalsPerMeasure.put(measure, missingIntervalsForMeasure);
        }
        LOG.info("Prefetching: {}", missingIntervalsPerMeasure);
        if(missingIntervalsPerMeasure.isEmpty()) return;
        Map<Integer, List<TimeSeriesSpan>> missingTimeSeriesSpanPerMeasure =
                dataProcessor.getMissing(from, to, missingIntervalsPerMeasure, aggFactors, viewPort, queryMethod);
        for(int measureWithMiss : missingTimeSeriesSpanPerMeasure.keySet()) {
            cacheManager.addToCache(missingTimeSeriesSpanPerMeasure.get(measureWithMiss));
        }
        LOG.info("Inserted new time series spans into interval tree");
    }
}
