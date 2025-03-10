package gr.imsi.athenarc.visual.middleware.datasource.query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

public class SQLQuery extends DataSourceQuery {

    final String schema;
    final String table;
    final String timeCol;
    final String idCol;
    final String format;
    final String valueCol;

    public SQLQuery(String schema, String table, String format, String timeCol, String idCol, String valueCol, long from, long to, Map<String, List<TimeInterval>> missingTimeIntervalsPerMeasure){
        super(from, to, missingTimeIntervalsPerMeasure);
        this.schema = schema;
        this.table = table;
        this.timeCol = timeCol;
        this.idCol = idCol;
        this.valueCol = valueCol;
        this.format = format;
    }

    public SQLQuery(String schema, String table, String format, String timeCol, String idCol, String valueCol, long from, long to, Map<String, List<TimeInterval>> missingTimeIntervalsPerMeasure, Map<String, Integer> numberOfGroupsPerMeasure) {
        super(from, to, missingTimeIntervalsPerMeasure, numberOfGroupsPerMeasure);
        this.schema = schema;
        this.table = table;
        this.timeCol = timeCol;
        this.idCol = idCol;
        this.valueCol = valueCol;
        this.format = format;
    }

    private String calculateFilter(TimeInterval range, String measure) {
        return  " (" + timeCol + " >= " + "'" + range.getFromDate(format)  + "'" + " AND " + timeCol + " < " + "'" + range.getToDate(format)  + "'" + " AND " + idCol+ " = '" + measure + "' ) \n" ;
    }

    private String rawSkeleton(TimeInterval range, String measure, int i){
        return "SELECT " + idCol + " , " + timeCol + " , " + valueCol + " ," + i + " as u_id FROM " + schema + "." + table + " \n" +
                "WHERE " +
                calculateFilter(range, measure);
    }

    private String rawQuerySkeletonCreator() {
        return null;
//        return IntStream.range(0, ranges.size()).mapToObj(idx -> {
////            TimeInterval range = ranges.get(idx);
////            List<Integer> measureOfRange = measures.get(idx);
//            return measureOfRange.stream().map(m -> rawSkeleton(range, m, idx)).collect(Collectors.joining(" UNION ALL "));
//        }).collect(Collectors.joining(" UNION ALL "));
    }

    private String m4Skeleton(TimeInterval range, String measure, int width, int i ){
       return "SELECT Q." + idCol + " , " +  timeCol + " , " + valueCol + " , k, " + i + " as u_id \n" +
                "FROM " + schema + "." + table + " as Q \n" +
                "JOIN " +
                "(SELECT " + idCol + " , floor( \n" +
                "((EXTRACT(EPOCH FROM " + timeCol + ") * 1000) - " + from + " ) / ((" + to + " - " + from + " ) / " + width + " )) as k, \n" +
                "min(" + valueCol + " ) as v_min, max(" + valueCol + " ) as v_max, \n"  +
                "min(" + timeCol + " ) as t_min, max(" + timeCol + " ) as t_max \n"  +
                "FROM " + schema + "." + table + " \n" +
                "WHERE \n" +
                calculateFilter(range, measure) +
                "GROUP BY " + idCol + " , k ) as QA \n"+
                "ON k = floor(((EXTRACT(EPOCH FROM " + timeCol + ") * 1000) - " + from + " ) / ((" + to + " - " + from  + ") / " + width + " )) \n" +
                "AND QA." + idCol + " = " + "Q." + idCol + " \n" +
                "AND (" + valueCol + " = v_min OR " + valueCol + " = v_max OR \n" +
                timeCol + " = t_min OR " + timeCol + " = t_max) \n" +
                "WHERE \n"  +
                "(" + timeCol + " >= " + "'" + range.getFromDate(format) + "'" + " AND " + timeCol + " < " + "'" + range.getToDate(format) + "'" + " AND QA." + idCol + " = '" + measure + "' ) \n" ;
    }

    private String m4QuerySkeletonCreator() {
        AtomicInteger idx = new AtomicInteger();
        return missingIntervalsPerMeasure.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(range -> m4Skeleton(range, entry.getKey(), numberOfGroups.get(entry.getKey()), idx.getAndIncrement()))
                )
                .collect(Collectors.joining(" UNION ALL "));

    }

    private String minMaxSkeleton(TimeInterval range, String measure, int i ) {
        return "SELECT " + idCol + " , floor( \n" +
                "((EXTRACT(epoch FROM " + timeCol + " ) * 1000) - " + range.getFrom() + " ) / " + aggregateIntervals.get(measure) + ") as k, \n" +
                "min(" + valueCol + " ) as v_min, max(" + valueCol + " ) as v_max, "  + i + " as u_id \n" +
                "FROM " + schema + "." + table + " \n" +
                "WHERE " +
                calculateFilter(range, measure) +
                "GROUP BY " + idCol + " , k \n";
    }

    private String minMaxQuerySkeletonCreator() {
        AtomicInteger idx = new AtomicInteger();
        return missingIntervalsPerMeasure.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(range -> minMaxSkeleton(range, entry.getKey(), idx.getAndIncrement()))
                )
                .collect(Collectors.joining(" UNION ALL "));

    }

    @Override
    public String rawQuerySkeleton() {
        return rawQuerySkeletonCreator() +
                "ORDER BY u_id, " + timeCol + " , " + idCol + "\n";
    }

    @Override
    public String m4QuerySkeleton() {
        return "WITH Q_M AS (" + m4QuerySkeletonCreator() + ") \n" +
                "SELECT " + idCol + " , EXTRACT(EPOCH FROM MIN(" + timeCol + ")) * 1000 AS min_time , EXTRACT(EPOCH FROM MAX(" + timeCol + ")) * 1000 AS max_time, " + valueCol + " , k, u_id FROM Q_M \n" +
                "GROUP BY " + idCol + " , k , " + valueCol + " , u_id \n" +
                "ORDER BY u_id, k, " + idCol;
    }

    @Override
    public String minMaxQuerySkeleton() {
        return minMaxQuerySkeletonCreator() +
                " ORDER BY u_id, k, " + idCol;
    }
}
