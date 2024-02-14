package gr.imsi.athenarc.visual.middleware.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class MultiSpanIterator<T> implements Iterator<T>, Cloneable {

    private final Iterator<Iterator<T>> iteratorChain;
    private final Iterator<Iterable<T>> iterableChain;
    private Iterator<T> currentIterator;
    private Iterable<T> currentIterable;
    private Iterator<T> lastIterator;

    private Consumer<T>[] consumers;

    public MultiSpanIterator(Iterator<Iterable<T>> iterator, Consumer<T>... consumers) {
        List<Iterator<T>> iteratorList = new ArrayList<>();
        List<Iterable<T>> iterablesList = new ArrayList<>();
        for (Iterator<Iterable<T>> it = iterator; it.hasNext(); ) {
            Iterable iterable = it.next();
            Iterator<T> iterator1 = iterable.iterator();
            iterablesList.add(iterable);
            iteratorList.add(iterator1);
        }
        this.iteratorChain = iteratorList.iterator();
        this.iterableChain = iterablesList.iterator();
        this.consumers = consumers;
    }


    @Override
    public boolean hasNext() {
        while (currentIterator == null || !currentIterator.hasNext()) {
            if (!iteratorChain.hasNext()) return false;
            currentIterator = iteratorChain.next();
            currentIterable = iterableChain.next();
        }
        return true;
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            this.lastIterator = null;         // to disallow remove()
            throw new NoSuchElementException();
        }
        this.lastIterator = currentIterator;  // to support remove()
        T next = currentIterator.next();
        for (Consumer<T> consumer : consumers) {
            consumer.accept(next);
        }
        return next;
    }


    @Override
    public void remove() {
        if (this.lastIterator == null) {
            throw new IllegalStateException();
        }
        this.lastIterator.remove();
    }

    public Iterator<T> getCurrentIterator() {
        return currentIterator;
    }

    public Iterable<T> getCurrentIterable() {
        return currentIterable;
    }

}
