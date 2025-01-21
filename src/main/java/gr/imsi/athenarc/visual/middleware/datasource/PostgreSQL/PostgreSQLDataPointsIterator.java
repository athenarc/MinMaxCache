package gr.imsi.athenarc.visual.middleware.datasource.postgresql;

import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class PostgreSQLDataPointsIterator implements Iterator<DataPoint> {

    private final ResultSet resultSet;
    private final List<Integer> measures;
    private int unionGroup = 0;

    public PostgreSQLDataPointsIterator(List<Integer> measures, ResultSet resultSet) {
        this.measures = measures;
        this.resultSet = resultSet;
        this.unionGroup = 0; // signifies the union id
    }

    @Override
    public boolean hasNext() {
        try {
            return !(resultSet.isAfterLast());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public ImmutableDataPoint next() {
//        int currentUnionGroup = unionGroup;
//        double[] values = new double[measures.get(unionGroup).size()];
//        long datetime = 0L;
//        try {
//            int i = 0;
//            while (currentUnionGroup == unionGroup && i < measures.get(unionGroup).size() && resultSet.next()) {
//                datetime = resultSet.getLong(2);
//                Double val = resultSet.getObject(3) == null ? null : resultSet.getDouble(3);
//                currentUnionGroup = unionGroup;
//                unionGroup = resultSet.getInt(4);
//                if(val == null) {
//                    i++;
//                    continue;
//                }
//                values[i] = val;
//                i ++;
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return new ImmutableDataPoint(datetime, values);
        return null;
    }
}
