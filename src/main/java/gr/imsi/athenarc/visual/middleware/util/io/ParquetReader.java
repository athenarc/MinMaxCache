// package gr.imsi.athenarc.visual.middleware.util.io;

// import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.parquet.column.ColumnDescriptor;
// import org.apache.parquet.column.page.PageReadStore;
// import org.apache.parquet.example.data.Group;
// import org.apache.parquet.example.data.simple.SimpleGroup;
// import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
// import org.apache.parquet.hadoop.ParquetFileReader;
// import org.apache.parquet.hadoop.util.HadoopInputFile;
// import org.apache.parquet.io.ColumnIOFactory;
// import org.apache.parquet.io.MessageColumnIO;
// import org.apache.parquet.io.RecordReader;
// import org.apache.parquet.schema.MessageType;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import java.io.IOException;
// import java.time.*;
// import java.time.format.DateTimeFormatter;
// import java.time.temporal.ChronoUnit;
// import java.util.*;
// import java.util.stream.Collectors;

// public class ParquetReader {

//     private static final Logger LOG = LoggerFactory.getLogger(ParquetReader.class);

//     private final String filePath;
//     private final ParquetFileReader reader;
//     private final MessageType schema;
//     private final DateTimeFormatter formatter;
//     private final MessageColumnIO columnIO;
//     private final String timeCol;
//     private final Integer timeColIndex;

//     private long startTime;
//     private long endTime;
//     private String[] parsedHeader;
//     private Duration samplingInterval;
//     private List<Integer> measures;
//     private long partitionSize;
//     private long currentPartitionSize;
//     private long currentRowGroupOffset;
//     private int currentRowGroupId;
//     private RecordReader<Group> recordReader;

//     public ParquetReader(String filePath,  DateTimeFormatter formatter, String timeCol) throws IOException {
//         this(filePath, formatter, timeCol, null);
//     }

//     public ParquetReader(String filePath,  DateTimeFormatter formatter, String timeCol, Duration samplingInterval) throws IOException {
//         this.filePath = filePath;
//         this.formatter = formatter;
//         this.reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(this.filePath), new Configuration()));
//         this.schema = reader.getFooter().getFileMetaData().getSchema();
//         this.columnIO = new ColumnIOFactory().getColumnIO(this.schema);
//         this.parsedHeader = this.schema.getColumns().stream().map(ColumnDescriptor::toString).toArray(String[]::new);
//         this.timeCol = timeCol;
//         this.timeColIndex = Arrays.stream(this.parsedHeader).collect(Collectors.toList()).indexOf(timeCol);

//         computeFileStats(samplingInterval);

//         LOG.info("Created reader for file {} with {} partition size.", filePath, this.partitionSize);
//     }
//     private void computeFileStats(Duration samplingInterval) throws IOException {
//         PageReadStore firstRowGroup = reader.readRowGroup(0);
//         this.partitionSize = firstRowGroup.getRowCount();
//         RecordReader firstRowGroupReader = columnIO.getRecordReader(firstRowGroup, new GroupRecordConverter(schema));
//         SimpleGroup row = (SimpleGroup) firstRowGroupReader.read();
//         this.startTime =  parseStringToTimestamp(row.getBinary(timeCol, 0).toStringUsingUTF8()); // Get date col
//         if(this.samplingInterval != null) return;
//         row = (SimpleGroup) firstRowGroupReader.read();
//         long secondTime =  parseStringToTimestamp(row.getBinary(timeCol, 0).toStringUsingUTF8());
//         this.samplingInterval = Duration.of(startTime - secondTime, ChronoUnit.MILLIS);

//         PageReadStore lastRowGroup = reader.readRowGroup(this.reader.getRowGroups().size() - 1);
//         RecordReader lastRowGroupReader = columnIO.getRecordReader(lastRowGroup, new GroupRecordConverter(schema));
//         int i = 0;
//         while(i ++ < lastRowGroup.getRowCount()) row = (SimpleGroup) lastRowGroupReader.read();
//         this.endTime =  parseStringToTimestamp(row.getBinary(timeCol, 0).toStringUsingUTF8());
//     }


//     public long findPosition(long time) { return Duration.of(startTime - time, ChronoUnit.MILLIS).dividedBy(samplingInterval);}

//     private void readRowGroup(int rowGroupId) throws IOException {
//         PageReadStore rowGroup = reader.readRowGroup(rowGroupId);
//         recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
//         currentRowGroupOffset = 0;
//         currentRowGroupId = rowGroupId;
//     }

//     private void readRowGroup(int rowGroupId, long rowGroupOffset) throws IOException {
//         PageReadStore rowGroup = reader.readRowGroup(rowGroupId);
//         recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
//         currentRowGroupOffset = rowGroupOffset;
//         currentRowGroupId = rowGroupId;
//         long id = 0;
//         while(id ++ < currentRowGroupOffset) recordReader.read();
//     }

//     private int findProbabilisticRowGroupID(long time) {
//         long index = findPosition(time);
//         return (int) Math.floorDiv(index, partitionSize);
//     }

//     private void seekPosition(long timestamp) throws IOException {
//         readRowGroup(findProbabilisticRowGroupID(timestamp));
//         if(hasNext()) {
//             long foundTimestamp = parseStringToTimestamp(parseNext()[0]);
//             if (foundTimestamp <= timestamp) seekPositionForward(timestamp);
//             else seekPositionBackward(timestamp);
//         }
//     }

//     private void seekPositionForward(long timestamp) throws IOException {
//         long time = 0;
//         while(hasNext() && time < timestamp){
//             time = parseStringToTimestamp(parseNext()[0]);
//         }
//         if(currentRowGroupOffset == 0 ) {
//             currentRowGroupId --;
//             currentRowGroupOffset = partitionSize - 1;
//         }
//         else currentRowGroupOffset --;
//     }

//     private void seekPositionBackward(long timestamp) throws IOException {
//         long time = timestamp + 1;
//         while(time >= timestamp){
//             readRowGroup(--currentRowGroupId);
//             time = parseStringToTimestamp(parseNext()[0]);
//         }
//         seekPositionForward(timestamp);
//     }


//     public void seekTimestamp(long timestamp) throws IOException {
//         seekPosition(timestamp);
//         readRowGroup(currentRowGroupId, currentRowGroupOffset);
//     }

//     public String[] parseNext() throws IOException {
//         SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
//         String[] row = new String[measures.size() + 1];
//         String time = simpleGroup.getBinary(timeCol, 0).toStringUsingUTF8();
//         row[0] = time;
//         int j = 1;
//         for (Integer column : measures){
//             row[j++] = String.valueOf(simpleGroup.getDouble(column, 0));
//         }
//         currentRowGroupOffset ++;
//         return row;
//     }

//     public boolean hasNext() throws IOException {
//         if (currentRowGroupOffset  == currentPartitionSize) {
//             PageReadStore rowGroup = reader.readRowGroup(++currentRowGroupId);
//             if (rowGroup != null) {
//                 currentRowGroupOffset = 0;
//                 currentPartitionSize = rowGroup.getRowCount();
//                 recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
//                 return true;
//             }
//             return false;
//         }
//         return true;
//     }

//     public long parseStringToTimestamp(String s) {
//         ZonedDateTime zonedDateTime = LocalDateTime.parse(s, formatter).atZone(ZoneId.of("UTC"));
//         return zonedDateTime.toInstant().toEpochMilli();
//     }

//     public String parseTimestampToString(long t) {
//         return formatter.format(Instant.ofEpochMilli(t).atZone(ZoneId.of("UTC")).toLocalDateTime());
//     }

//     public String[] getParsedHeader() {
//         return parsedHeader;
//     }

//     public Duration getSamplingInterval() {
//         return samplingInterval;
//     }

//     public TimeRange getTimeRange() {
//         return new TimeRange(this.startTime, this.endTime);
//     }

//     public String getTimeCol() {
//         return timeCol;
//     }

//     public Integer getTimeColIndex() {
//         return timeColIndex;
//     }

//     public List<Integer> getMeasures() {
//         return measures;
//     }

//     public void close() throws IOException { reader.close(); }

//     static final class ParquetOffset {
//         long rowGroupId;
//         long rowGroupOffset;
//         RecordReader<Group> rowGroupReader;

//         ParquetOffset(long rowGroupId, long rowGroupOffset) {
//             this.rowGroupId = rowGroupId;
//             this.rowGroupOffset = rowGroupOffset;

//         }

//         @Override
//         public String toString() {
//             return "ParquetOffset{" +
//                     "rowGroupId=" + rowGroupId +
//                     ", rowGroupOffset=" + rowGroupOffset +
//                     '}';
//         }
//     }

// }
