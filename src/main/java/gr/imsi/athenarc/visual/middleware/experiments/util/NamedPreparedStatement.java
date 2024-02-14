package gr.imsi.athenarc.visual.middleware.experiments.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class NamedPreparedStatement {

    private final PreparedStatement prepStmt;
    private final HashMap<String, ArrayList<Integer>> fields = new HashMap<>();
    private int count = 0;

    public NamedPreparedStatement(Connection conn, String sql) throws SQLException {
        int pos;
        while((pos = sql.indexOf("$")) != -1) {
            int end = sql.substring(pos).indexOf(" ");
            if (end == -1)
                end = sql.length();
            else
                end += pos;
            String token = sql.substring(pos + 1,end);
            ArrayList<Integer> ids = fields.getOrDefault(token, new ArrayList<>());
            ids.add(count += 1);
            fields.put(token, ids);
            sql = sql.substring(0, pos) + "?" + sql.substring(end);
        }
        prepStmt = conn.prepareStatement(sql);
    }

    public PreparedStatement getPreparedStatement() {
        return prepStmt;
    }

    public void close() throws SQLException {
        prepStmt.close();
    }

    public void setInt(String name, int value) throws SQLException {
        for(Integer idx : getIndexes(name))
            prepStmt.setInt(idx, value);
    }

    public void setLong(String name, long value) throws SQLException {
        for(Integer idx : getIndexes(name))
            prepStmt.setLong(idx, value);
    }

    public void setString(String name, String value) throws SQLException {
        for(Integer idx : getIndexes(name))
            prepStmt.setString(idx, value);
    }

    private ArrayList<Integer> getIndexes(String name) {
        return fields.get(name);
    }

}