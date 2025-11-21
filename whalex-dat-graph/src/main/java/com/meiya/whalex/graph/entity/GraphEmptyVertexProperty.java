package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.util.GraphStringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 空节点属性对象
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 */
public final class GraphEmptyVertexProperty<V> implements GraphVertexProperty<V> {
    private static final GraphEmptyVertexProperty INSTANCE = new GraphEmptyVertexProperty();

    private GraphEmptyVertexProperty() {
    }

    @Override
    public String key() {
        throw new IllegalStateException("The property does not exist as it has no key, value, or associated element");
    }

    @Override
    public V value() throws NoSuchElementException {
        throw new IllegalStateException("The property does not exist as it has no key, value, or associated element");
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public GraphVertex element() {
        throw new IllegalStateException("The property does not exist as it has no key, value, or associated element");
    }

    @Override
    public Object id() {
        return null;
    }

    @Override
    public <U> Iterator<GraphProperty<U>> properties(String... propertyKeys) {
        return Collections.emptyIterator();
    }

    @Override
    public String toString() {
        return GraphStringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof GraphEmptyProperty;
    }

    @Override
    public int hashCode() {
        return 1281483122;
    }

    public static <V> GraphVertexProperty<V> instance() {
        return INSTANCE;
    }
}
