package com.meiya.whalex.graph.entity;

import java.util.Iterator;

/**
 * 节点对象
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 */
public interface GraphVertex extends GraphElement {

    String DEFAULT_LABEL = "vertex";
    Object[] EMPTY_ARGS = new Object[0];

    /**
     * 属性
     *
     * @param key
     * @param <V>
     * @return
     */
    @Override
    default <V> GraphVertexProperty<V> property(final String key) {
        Iterator<GraphVertexProperty<V>> iterator = this.properties(key);
        if (iterator.hasNext()) {
            GraphVertexProperty<V> property = iterator.next();
            if (iterator.hasNext()) {
                throw new IllegalStateException("Multiple properties exist for the provided key, use Vertex.properties(" + key + ')');
            } else {
                return property;
            }
        } else {
            return GraphVertexProperty.empty();
        }
    }

    /**
     * 设置节点属性
     *
     * @param cardinality
     * @param key
     * @param value
     * @param keyValues
     * @param <V>
     * @return
     */
    <V> GraphVertexProperty<V> property(final GraphVertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues);

    /**
     * 获取边
     *
     * @param direction
     * @param edgeLabels
     * @return
     */
    Iterator<GraphEdge> edges(final GraphDirection direction, final String... edgeLabels);

    /**
     * 获取与当前节点关联的节点
     *
     * @param direction
     * @param edgeLabels
     * @return
     */
    Iterator<GraphVertex> vertices(final GraphDirection direction, final String... edgeLabels);

    @Override
    <V> Iterator<GraphVertexProperty<V>> properties(final String... propertyKeys);

}
