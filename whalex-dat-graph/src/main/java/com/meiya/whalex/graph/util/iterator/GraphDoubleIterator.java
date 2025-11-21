package com.meiya.whalex.graph.util.iterator;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Double 类型迭代器
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.util
 * @project whalex-data-driver
 */
public class GraphDoubleIterator<T> implements Iterator<T>, Serializable {

    private T a;
    private T b;
    private char current = 'a';

    protected GraphDoubleIterator(final T a, final T b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean hasNext() {
        return this.current != 'x';
    }

    @Override
    public void remove() {
        if (this.current == 'b') {
            this.a = null;
        } else if (this.current == 'x') {
            this.b = null;
        }

    }

    @Override
    public T next() {
        if (this.current == 'x') {
            throw FastNoSuchElementException.instance();
        } else if (this.current == 'a') {
            this.current = 'b';
            return this.a;
        } else {
            this.current = 'x';
            return this.b;
        }
    }

}
