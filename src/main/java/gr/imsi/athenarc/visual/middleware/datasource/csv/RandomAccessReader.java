package gr.imsi.athenarc.visual.middleware.datasource.csv;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;


public class RandomAccessReader extends RandomAccessFile {


    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65536;
    // channel liked with the file, used to retrieve data and force updates.
    protected final FileChannel channel;
    protected final String filePath;
    // absolute filesystem path to the file
    private final long bufferSize;
    private final long fileLength;
    private final Charset charset;
    private DIRECTION direction;

    // buffer which will cache file blocks
    protected byte[] buffer;
    // `current` as current position in file
    // `bufferOffset` is the offset of the beginning of the buffer
    // `markedPointer` folds the offset of the last file mark
    // `lastRowReadOffset` as position of last row read
    protected long bufferOffset, current = 0, lastRowReadOffset = 0, markedPointer;
    // `validBufferBytes` is the number of bytes in the buffer that are actually valid;
    //  this will be LESS than buffer capacity if buffer is not full!
    protected int validBufferBytes;

    protected int rebufferCount = 0;
    private RandomAccessReader(File file, int bufferSize, Charset charset) throws IOException {
        super(file, "r");
        channel = super.getChannel();
        filePath = file.getAbsolutePath();
        this.charset = charset;
        this.direction = DIRECTION.FORWARD;
        this.bufferSize = bufferSize;
        // allocating required size of the buffer
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");
        buffer = new byte[bufferSize];


        // we can cache file length in read-only mode
        fileLength = channel.size();
        validBufferBytes = -1; // that will trigger reBuffer() on demand by read/seek operations
    }

    public  RandomAccessReader(File file) throws IOException {
        this(file, DEFAULT_BUFFER_SIZE, (Charset) Charset.defaultCharset());
    }

    public RandomAccessReader(File file, Charset charset) throws IOException {
        this(file, DEFAULT_BUFFER_SIZE, charset);
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     *
     * @throws IOException on any I/O error.
     */
    protected void reBuffer() throws IOException {
        rebufferCount++;
        resetBuffer();
        if (bufferOffset >= channel.size() || bufferOffset < 0) return;
    
        int read = 0;
        channel.position(bufferOffset);
    
        while (read < buffer.length) {
            int n = super.read(buffer, read, buffer.length - read);
            if (n < 0) break;
            read += n;
        }
        validBufferBytes = read;
    
        // Adjust `current` for the beginning of the file in backward direction
        if (direction == DIRECTION.BACKWARD && current < bufferOffset) {
            current = bufferOffset + validBufferBytes;
        }
    }
    

    @Override
    public long getFilePointer() {
        return current;
    }

    public String getPath() {
        return filePath;
    }

    public void reset() throws IOException {
        seek(markedPointer);
    }

    public long bytesPastMark() {
        long bytes = current - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    /**
     * @return true if there is no more data to read
     */
    public boolean isEOF() {
        return getFilePointer() == length();
    }

    public long bytesRemaining() {
        return length() - getFilePointer();
    }

    protected int bufferCursor() {
        return (int) (current - bufferOffset);
    }

    protected void resetBuffer() throws IOException {
        if (direction == DIRECTION.FORWARD) {
            bufferOffset = current;
        } else {
            // Handle edge case for start of file when reading backward
            bufferOffset = Math.max(0, current - bufferSize);
        }
        validBufferBytes = 0;
    }
    

    @Override
    public void close() throws IOException {
        buffer = null;
        super.close();
    }


    @Override
    public void seek(long newPosition) throws IOException {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (newPosition > length()) // it is save to call length() in read-only mode
            throw new EOFException(String.format("unable to seek to position %d in %s (%d bytes) in read-only mode",
                newPosition, getPath(), length()));

        current = newPosition;
        if (newPosition > (bufferOffset + validBufferBytes) || newPosition < (bufferOffset - validBufferBytes))
            reBuffer();
    }

    // @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    // public int read() throws IOException {
    //     if (buffer == null)
    //         throw new ClosedChannelException();

    //     if (isEOF())
    //         return -1; // required by RandomAccessFile

    //     if(direction == DIRECTION.BACKWARD){
    //         if(current <= bufferOffset) reBuffer();
    //         return ((int) buffer[(int) ((--current - bufferOffset))]) & 0xff;
    //     }
    //     else{
    //         if (current >= bufferOffset + buffer.length || validBufferBytes == -1) reBuffer();
    //         return ((int) buffer[(int) ((current++ - bufferOffset))]) & 0xff;
    //     }
    // }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read() throws IOException {
        if (buffer == null)
            throw new ClosedChannelException();

        if (isEOF())
            return -1; // required by RandomAccessFile

        if (direction == DIRECTION.BACKWARD) {
            if (current <= bufferOffset) reBuffer();

            // Check if we have valid data to read
            if (validBufferBytes == 0 || current <= bufferOffset) {
                return -1; // No more data to read
            }

            int index = (int) (--current - bufferOffset);
            if (index < 0 || index >= validBufferBytes) {
                return -1;
            }
            return ((int) buffer[index]) & 0xff;
        } else {
            if (current >= bufferOffset + validBufferBytes || validBufferBytes == -1) reBuffer();

            // Check if we have valid data to read
            if (validBufferBytes == 0 || current - bufferOffset >= validBufferBytes) {
                return -1; // No more data to read
            }

            int index = (int) (current - bufferOffset);
            if (index < 0 || index >= validBufferBytes) {
                return -1;
            }
            current++;
            return ((int) buffer[index]) & 0xff;
        }
    }

    @Override
    public int read(byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length) throws IOException {
        if (buffer == null)
            throw new ClosedChannelException();

        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (current >= bufferOffset + buffer.length || validBufferBytes == -1)
            reBuffer();

        assert current >= bufferOffset && current < bufferOffset + validBufferBytes
            : String.format("File (%s), current offset %d, buffer offset %d, buffer limit %d",
            getPath(),
            current,
            bufferOffset,
            validBufferBytes);

        int toCopy = Math.min(length, validBufferBytes - bufferCursor());
        System.arraycopy(buffer, bufferCursor(), buff, offset, toCopy);
        current += toCopy;

        return toCopy;
    }
    

    public ByteBuffer readBytes(int length) throws IOException {
        assert length >= 0 : "buffer length should not be negative: " + length;

        byte[] buff = new byte[length];
        readFully(buff); // reading data buffer

        return ByteBuffer.wrap(buff);
    }

    public final String readLineReverse() throws IOException {
        StringBuilder input = new StringBuilder();
        int c = -1;
        boolean eol = false;
    
        while (!eol) {
            if (current <= 0) return input.length() == 0 ? null : input.reverse().toString();
            c = read();
            switch (c) {
                case -1:
                    eol = true;
                    break;
                case '\n':
                    eol = true;
                    break;
                case '\r':
                    eol = true;
                    long cur = getFilePointer();
                    if ((read()) != '\n') {
                        seek(cur);
                    }
                    break;
                default:
                    input.append((char) c);
                    break;
            }
        }
    
        if ((c == -1) && (input.length() == 0)) {
            return null;
        }
        return input.reverse().toString();
    }

    public final String readNewLine() throws IOException {
        if(this.direction == DIRECTION.FORWARD) {
            this.lastRowReadOffset = this.getFilePointer() - 1;
            return this.readLine();
        }
        return this.readLineReverse();
    }

    public long getLastRowReadOffset() {
        return lastRowReadOffset;
    }

    @Override
    public long length() {
        return fileLength;
    }

    @Override
    public void write(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] buffer, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    public Charset getCharset() {
        return this.charset;
    }

    public void setDirection(DIRECTION direction) throws IOException {
        this.direction = direction;
        if(this.direction == DIRECTION.BACKWARD) current --;
    }

    public DIRECTION getDirection() {
        return direction;
    }

    
}
