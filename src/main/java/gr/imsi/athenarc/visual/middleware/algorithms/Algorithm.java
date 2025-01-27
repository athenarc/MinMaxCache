package gr.imsi.athenarc.visual.middleware.algorithms;

import java.util.Map;

import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import gr.imsi.athenarc.visual.middleware.web.rest.model.VisualQuery;
import gr.imsi.athenarc.visual.middleware.datasource.connector.DatasourceConnector;

/**
 * Common interface for all query algorithms.
 */
public interface Algorithm {
    /**
     * Initialize the algorithm once. This can be where you build caches or set up
     * data structures, using the parameters given.
     *
     * @param schema             Database schema or measurement name
     * @param datasetId          Unique dataset/table identifier
     * @param influxDBConnector  Connector to InfluxDB
     * @param params             Extra parameters for the algorithm (accuracy, etc.)
     */
    void initialize(String schema, String datasetId, DatasourceConnector datasourceConnector, Map<String, String> params);

    /**
     * Execute a query using this algorithm.
     *
     * @param query Query object with from/to, measures, filter, etc.
     * @return QueryResults
     */
    QueryResults executeQuery(VisualQuery query);

    boolean isInitialized(String datasetId);
}
