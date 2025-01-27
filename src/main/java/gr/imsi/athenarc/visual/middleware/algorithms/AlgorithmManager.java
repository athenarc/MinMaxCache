package gr.imsi.athenarc.visual.middleware.algorithms;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import gr.imsi.athenarc.visual.middleware.datasource.connector.DatasourceConnector;

public class AlgorithmManager {
    private static final ConcurrentHashMap<String, Algorithm> algorithmInstances = new ConcurrentHashMap<>();

    // Method to get or initialize an algorithm
    public static Algorithm getOrInitializeAlgorithm(String algorithmName, String schema, String datasetId, DatasourceConnector connector, Map<String, String> params) {
        String key = generateKey(algorithmName, datasetId);

        return algorithmInstances.computeIfAbsent(key, k -> {
            Algorithm algorithm;
            if (algorithmName.contains("MinMaxCache")) {
                algorithm = new MinMaxCacheAlgorithm();
                algorithm.initialize(schema, datasetId, connector, params);
            } else if (algorithmName.contains("M4")) {
                algorithm = new M4Algorithm();
                algorithm.initialize(schema, datasetId, connector, params);
            } else {
                throw new IllegalArgumentException("Unsupported algorithm: " + algorithmName);
            }
            return algorithm;
        });
    }

    // Helper method to generate a unique key for each algorithm
    private static String generateKey(String algorithmName, String datasetId) {
        return algorithmName + "-" + datasetId;
    }

    // Optionally, clear algorithm instances (e.g., for cleanup or eviction)
    public static void clearAlgorithm(String algorithmName, String datasetId) {
        String key = generateKey(algorithmName, datasetId);
        algorithmInstances.remove(key);
    }

    public static void clearAll() {
        algorithmInstances.clear();
    }
}
