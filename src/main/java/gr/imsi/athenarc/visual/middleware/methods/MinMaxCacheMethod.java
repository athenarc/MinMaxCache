package gr.imsi.athenarc.visual.middleware.methods;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.cache.MinMaxCache;
import gr.imsi.athenarc.visual.middleware.cache.MinMaxCacheBuilder;
import gr.imsi.athenarc.visual.middleware.cache.query.Query;
import gr.imsi.athenarc.visual.middleware.datasource.connector.DatasourceConnector;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;

public class MinMaxCacheMethod implements Method {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCacheMethod.class);

    private int aggFactor;
    private int dataReductionRatio;
    private float prefetchingFactor;


    // Map to hold the minmaxcache of each dataset
    private final ConcurrentHashMap<String, MinMaxCache> cacheMap = new ConcurrentHashMap<>();
   
    @Override
    public void initialize(String schema, String datasetId, DatasourceConnector datasourceConnector, Map<String, String> params) {
        LOG.info("Initializing MinMaxCacheMethod for dataset = {}", datasetId);

        // Extract initialization parameters from the 'params' map as needed
        initializeInitParameters(params);
        
        // Build and store the MinMaxCache for the dataset
        cacheMap.computeIfAbsent(datasetId, key -> {
            return new MinMaxCacheBuilder()
                .setDatasourceConnector(datasourceConnector)
                .setSchema(schema)
                .setId(datasetId)
                .setPrefetchingFactor(prefetchingFactor)
                .setAggFactor(aggFactor)
                .setDataReductionRatio(dataReductionRatio)
                .build();
        });
    }

    @Override
    public VisualQueryResults executeQuery(VisualQuery visualQuery) {
        LOG.info("Executing MinMaxCache query for dataset = {}", visualQuery.getTable());
        MinMaxCache minMaxCache = cacheMap.get(visualQuery.getTable());
        if (minMaxCache == null) {
            throw new IllegalStateException("Method not initialized for dataset. Call initialize() first.");
        }
        long from = visualQuery.getFrom();
        long to = visualQuery.getTo();
        int width = visualQuery.getWidth();
        int height = visualQuery.getHeight();
        List<Integer> measures = visualQuery.getMeasures();
        Map<Integer, Double[]> filter = null;
        if (visualQuery.getParams().containsKey("accuracy")) {
            prefetchingFactor = Float.parseFloat(visualQuery.getParams().get("accuracy"));
        } else {
            throw new IllegalArgumentException("Missing accuracy query parameter for MinMaxCache method");
        }
        float accuracy = Float.parseFloat(visualQuery.getParams().get("accuracy"));

        Query minMaxCacheQuery = new Query(from, to, measures, accuracy, width, height, filter);    
        // Delegate to minMaxCache
        QueryResults minMaxCacheQueryResults =  minMaxCache.executeQuery(minMaxCacheQuery);

        VisualQueryResults visualQueryResults = new VisualQueryResults();
        visualQueryResults.setData(minMaxCacheQueryResults.getData());
        visualQueryResults.setTimeRange(minMaxCacheQueryResults.getTimeRange());
        visualQueryResults.setQueryTime(minMaxCacheQueryResults.getQueryTime());
        Map<String, String> metrics = new HashMap<>();
        metrics.put("error", minMaxCacheQueryResults.getError().toString());
        visualQueryResults.setMetrics(metrics);
        return visualQueryResults;
    }

    public void initializeInitParameters(Map<String, String> params){
        if (params.containsKey("prefetchingFactor")) {
            prefetchingFactor = Float.parseFloat(params.get("prefetchingFactor"));
        } else {
            throw new IllegalArgumentException("Missing prefetchingFactor parameter for MinMaxCache method");
        }

        if (params.containsKey("dataReductionRatio")) {
            dataReductionRatio = Integer.parseInt(params.get("dataReductionRatio"));
        } else {
            throw new IllegalArgumentException("Missing dataReductionRatio parameter for MinMaxCache method");
        }

        if (params.containsKey("aggFactor")) {
            aggFactor = Integer.parseInt(params.get("aggFactor"));
        } else {
            throw new IllegalArgumentException("Missing aggFactor parameter for MinMaxCache method");
        }
    }

    public boolean isInitialized(String datasetId) {
        return cacheMap.containsKey(datasetId);
    }
}
