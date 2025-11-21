package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.util.iterator.GraphIteratorUtils;

import java.util.*;
import java.util.function.Supplier;

/**
 * 图数据库对象基础元素
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 */
public interface GraphElement {

    /**
     * ID
     * @return
     */
    Object id();

    /**
     * 标签
     * @return
     */
    String label();

    /**
     * 键列表
     *
     * @return
     */
    default Set<String> keys() {
        Set<String> keys = new HashSet();
        this.properties().forEachRemaining((property) -> {
            keys.add(property.key());
        });
        return Collections.unmodifiableSet(keys);
    }

    /**
     * 属性
     *
     * @param key
     * @param <V>
     * @return
     */
    default <V> GraphProperty<V> property(final String key) {
        Iterator<? extends GraphProperty<V>> iterator = this.properties(key);
        return iterator.hasNext() ? iterator.next() : GraphProperty.empty();
    }

    /**
     * 获取值
     *
     * @param key
     * @param <V>
     * @return
     * @throws NoSuchElementException
     */
    default <V> V value(final String key) throws NoSuchElementException {
        return (V) this.property(key).orElseThrow(new Supplier<NoSuchElementException>() {
            @Override
            public NoSuchElementException get() {
                return new NoSuchElementException("The property does not exist as the key has no associated value for the provided element: " + this + ":" + key);
            }
        });
    }

    /**
     * 获取值列表
     *
     * @param propertyKeys
     * @param <V>
     * @return
     */
    default <V> Iterator<V> values(final String... propertyKeys) {
        return GraphIteratorUtils.map(this.properties(propertyKeys), (property) -> (V)((GraphProperty)property).value());
    }

    /**
     * 属性列表
     *
     * @param propertyKeys
     * @param <V>
     * @return
     */
    <V> Iterator<? extends GraphProperty<V>> properties(final String... propertyKeys);
}
