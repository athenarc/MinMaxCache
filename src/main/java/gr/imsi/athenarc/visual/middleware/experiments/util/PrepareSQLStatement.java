package gr.imsi.athenarc.visual.middleware.experiments.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrepareSQLStatement {
    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

    private final HashMap<String, ArrayList<Integer>> fields = new HashMap<>();
    private String sql;
    private String[] replacements;

    public PrepareSQLStatement(String sql) throws SQLException {
        this.sql = sql;
        initialize();
    }

    private void initialize() {
        int pos;
        int count = 0;
        while((pos = sql.indexOf(":")) != -1) {
            int end = sql.substring(pos).indexOf(" ");
            if (end == -1)
                end = sql.length();
            else
                end += pos;
            String token = sql.substring(pos + 1,end);
            ArrayList<Integer> ids = fields.getOrDefault(token, new ArrayList<>());
            ids.add(count+=1);
            fields.put(token, ids);
            sql = sql.substring(0, pos) + "?" + sql.substring(end);
        }
    }

    public String getSql() {
        return sql;
    }


    private void replaceIthQuestionMark(int i, String replacement) {
        // Construct the regular expression to match the ith occurrence of "?"
        String regex = "(\\?)";

        // Create a StringBuffer to build the result string
        StringBuffer result = new StringBuffer();

        // Create a counter to keep track of the "?" occurrences
        int count = 0;
        // Use a Matcher to find and replace the ith occurrence
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(regex).matcher(sql);
        while (matcher.find()) {
            count++;
            if (count == i) {
                // Replace the ith occurrence with the replacement string
                matcher.appendReplacement(result, replacement);
            } else {
                // Append the matched "?" to the result without replacement
                matcher.appendReplacement(result, matcher.group(1));
            }
        }
        for (Map.Entry<String, ArrayList<Integer>> entry : fields.entrySet()) {
            String key = entry.getKey();
            List<Integer> values = entry.getValue();

            // Iterate through the List<Integer> for each key
            for (int j = 0; j < values.size(); j++) {
                int value = values.get(j);
                if(value == i) values.set(j, -1);
                else if (value > i) {
                    // Decrement by 1 if the value is larger than 'i'
                    values.set(j, value - 1);
                }
            }
        }

        // Append the remaining part of the input string
        matcher.appendTail(result);
        sql = result.toString();
    }

    public void setInt(String name, int value) throws SQLException {
        for(Integer idx : getIndexes(name))
            replaceIthQuestionMark(idx, String.valueOf(value));
    }

    public void setLong(String name, long value) throws SQLException {
        for(Integer idx : getIndexes(name))
            replaceIthQuestionMark(idx, String.valueOf(value));
    }

    public void setString(String name, String value) throws SQLException {
        for(Integer idx : getIndexes(name))
            replaceIthQuestionMark(idx, value);
    }

    private ArrayList<Integer> getIndexes(String name) {
        return fields.get(name);
    }

}