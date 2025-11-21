package com.meiya.whalex.graph.entity;

import java.util.Iterator;

/**
 * 节点属性
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 */
public interface GraphVertexProperty<V> extends GraphProperty<V>, GraphElement {

    String DEFAULT_LABEL = "vertexProperty";

    /**
     * 当前节点属性指向的节点
     *
     * @return
     */
    @Override
    GraphVertex element();

    /**
     * 属性key
     *
     * @return
     */
    @Override
    default String label() {
        return this.key();
    }

    /**
     * 构建空对象
     *
     * @param <V>
     * @return
     */
    static <V> GraphVertexProperty<V> empty() {
        return GraphEmptyVertexProperty.instance();
    }

    /**
     * 属性集合
     * 
     * @param propertyKeys
     * @param <U>
     * @return
     */
    @Override
    <U> Iterator<GraphProperty<U>> properties(final String... propertyKeys);

    /**
     * 属性值类型
     */
    enum Cardinality {
        /**
         * 单数
         */
        single,
        /**
         * 复数
         */
        list,
        /**
         * 不可重复复数
         */
        set;

        Cardinality() {
        }
    }
}
