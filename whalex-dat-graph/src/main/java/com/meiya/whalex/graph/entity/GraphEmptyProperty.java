package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.util.GraphStringFactory;

import java.util.NoSuchElementException;

/**
 * 空属性对象
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 */
public final class GraphEmptyProperty<V> implements GraphProperty<V> {
    private static final GraphEmptyProperty INSTANCE = new GraphEmptyProperty();

    private GraphEmptyProperty() {
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
    public GraphElement element() {
        throw new IllegalStateException("The property does not exist as it has no key, value, or associated element");
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

    public static <V> GraphProperty<V> instance() {
        return INSTANCE;
    }
}
