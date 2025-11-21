package com.meiya.whalex.graph.util.iterator;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 数组迭代器
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.util
 * @project whalex-data-driver
 */
public final class GraphArrayIterator<T> implements Iterator<T>, Serializable {

    private final T[] array;
    private int current = 0;

    public GraphArrayIterator(final T[] array) {
        this.array = array;
    }

    @Override
    public boolean hasNext() {
        return this.current < this.array.length;
    }

    @Override
    public T next() {
        if (this.hasNext()) {
            ++this.current;
            return this.array[this.current - 1];
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public String toString() {
        return Arrays.asList(this.array).toString();
    }

}
