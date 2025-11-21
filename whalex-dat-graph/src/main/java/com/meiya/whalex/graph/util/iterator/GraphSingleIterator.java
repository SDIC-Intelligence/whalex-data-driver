package com.meiya.whalex.graph.util.iterator;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.io.Serializable;
import java.util.Iterator;

/**
 * 单数迭代器
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.util
 * @project whalex-data-driver
 */
public class GraphSingleIterator<T> implements Iterator<T>, Serializable {

    private T t;
    private boolean alive = true;

    protected GraphSingleIterator(final T t) {
        this.t = t;
    }

    @Override
    public boolean hasNext() {
        return this.alive;
    }

    @Override
    public void remove() {
        this.t = null;
    }

    @Override
    public T next() {
        if (!this.alive) {
            throw FastNoSuchElementException.instance();
        } else {
            this.alive = false;
            return this.t;
        }
    }

}
