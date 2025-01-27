package gr.imsi.athenarc.visual.middleware.web.rest.model;

import java.util.Map;

public class Algorithm {
    String name;
    Map<String, String> params;

    public Algorithm() {
    }

    public Algorithm(String name, Map<String, String> params) {
        this.name = name;
        this.params = params;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getParams() {
        return params;
    }


    public String toString() {
        return "Algorithm{" +
            "name='" + name + '\'' +
            ", params=" + params +
            '}';
    }
}
