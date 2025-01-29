package gr.imsi.athenarc.visual.middleware.web.rest.model;

import java.util.Map;

public class MethodConfig {
    String key;
    Map<String, String> params;

    public MethodConfig() {
    }

    public MethodConfig(String key, Map<String, String> params) {
        this.key = key;
        this.params = params;
    }

    public String getKey() {
        return key;
    }

    public Map<String, String> getParams() {
        return params;
    }


    public String toString() {
        return "Method{" +
            "key='" + key + '\'' +
            ", params=" + params +
            '}';
    }
}
