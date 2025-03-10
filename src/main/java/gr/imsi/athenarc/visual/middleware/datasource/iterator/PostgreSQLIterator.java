package gr.imsi.athenarc.visual.middleware.datasource.iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class PostgreSQLIterator<T> implements Iterator<T> {
    protected static final Logger LOG = LoggerFactory.getLogger(PostgreSQLIterator.class);

    protected final ResultSet resultSet;
    protected boolean hasNext;

    protected PostgreSQLIterator(ResultSet resultSet) {
        if (resultSet == null) {
            throw new IllegalArgumentException("ResultSet cannot be null");
        }
        this.resultSet = resultSet;
        this.hasNext = true;
    }

    @Override
    public boolean hasNext() {
        try {
            if (!hasNext) {
                return false;
            }
            hasNext = resultSet.next();
            return hasNext;
        } catch (SQLException e) {
            LOG.error("Error checking for next record", e);
            return false;
        }
    }

    @Override
    public T next() {
        if (!hasNext) {
            throw new NoSuchElementException("No more elements to iterate over");
        }
        return getNext();
    }

    protected abstract T getNext();
}
