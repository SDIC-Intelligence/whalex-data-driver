package com.meiya.whalex.graph.util.iterator;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 复数迭代器
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.util
 * @project whalex-data-driver
 */
public final class GraphMultiIterator<T> implements Iterator<T>, Serializable {

    private final List<Iterator<T>> iterators = new ArrayList();
    private int current = 0;

    public GraphMultiIterator() {
    }

    public void addIterator(final Iterator<T> iterator) {
        this.iterators.add(iterator);
    }

    @Override
    public boolean hasNext() {
        if (this.current >= this.iterators.size()) {
            return false;
        } else {
            for(Iterator currentIterator = (Iterator)this.iterators.get(this.current); !currentIterator.hasNext(); currentIterator = (Iterator)this.iterators.get(this.current)) {
                ++this.current;
                if (this.current >= this.iterators.size()) {
                    return false;
                }
            }

            return true;
        }
    }

    @Override
    public void remove() {
        ((Iterator)this.iterators.get(this.current)).remove();
    }

    @Override
    public T next() {
        if (this.iterators.isEmpty()) {
            throw FastNoSuchElementException.instance();
        } else {
            Iterator currentIterator;
            for(currentIterator = (Iterator)this.iterators.get(this.current); !currentIterator.hasNext(); currentIterator = (Iterator)this.iterators.get(this.current)) {
                ++this.current;
                if (this.current >= this.iterators.size()) {
                    throw FastNoSuchElementException.instance();
                }
            }

            return (T) currentIterator.next();
        }
    }

    public void clear() {
        this.iterators.clear();
        this.current = 0;
    }

}
