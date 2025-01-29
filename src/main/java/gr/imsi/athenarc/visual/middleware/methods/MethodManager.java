package gr.imsi.athenarc.visual.middleware.methods;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import gr.imsi.athenarc.visual.middleware.datasource.connector.DatasourceConnector;

public class MethodManager {
    private static final ConcurrentHashMap<String, Method> methodInstances = new ConcurrentHashMap<>();

    // Method to get or initialize an method
    public static Method getOrInitializeMethod(String methodName, String schema, String datasetId, DatasourceConnector connector, Map<String, String> params) {
        String key = generateKey(methodName, datasetId);
        
        return methodInstances.computeIfAbsent(key, k -> {
            Method method;
            if (methodName.contains("MinMaxCache")) {
                method = new MinMaxCacheMethod();
                method.initialize(schema, datasetId, connector, params);
            } else if (methodName.contains("M4")) {
                method = new M4Method();
                method.initialize(schema, datasetId, connector, params);
            } else {
                throw new IllegalArgumentException("Unsupported method: " + methodName);
            }
            return method;
        });
    }

    // Helper method to generate a unique key for each method
    private static String generateKey(String methodName, String datasetId) {
        return methodName + "-" + datasetId;
    }

    // Optionally, clear method instances (e.g., for cleanup or eviction)
    public static void clearMethod(String methodName, String datasetId) {
        String key = generateKey(methodName, datasetId);
        methodInstances.remove(key);
    }

    public static void clearAll() {
        methodInstances.clear();
    }
}
