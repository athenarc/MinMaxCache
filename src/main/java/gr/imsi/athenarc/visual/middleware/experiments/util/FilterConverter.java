package gr.imsi.athenarc.visual.middleware.experiments.util;

import com.beust.jcommander.IStringConverter;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class FilterConverter implements IStringConverter<Map<Integer, Double[]>> {

    @Override
    public Map<Integer, Double[]> convert(String s) {
        Map<Integer, Double[]> filterMap = new HashMap<>();
        String[] filters = s.split(",");
        for (String filter : filters) {
            Double[] filterParts = ArrayUtils.toObject(Arrays.stream(filter.split(":")).mapToDouble(Double::parseDouble).toArray());
            filterMap.put(Integer.parseInt(filterParts[0].toString()), Arrays.copyOfRange(filterParts, 1, filterParts.length - 1));
        }
        return filterMap;
    }
}
