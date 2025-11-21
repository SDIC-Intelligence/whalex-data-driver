package com.meiya.whalex.graph.entity;

import java.util.Iterator;

/**
 * 图数据库关系对象
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 */
public interface GraphEdge extends GraphElement {

    String DEFAULT_LABEL = "edge";

    /**
     * 边关联的节点
     *
     * @param direction 边方向
     * @return
     */
    Iterator<GraphVertex> vertices(final GraphDirection direction);

    /**
     * 边的结束节点
     *
     * e -> v
     *
     * @return
     */
    GraphVertex outVertex();

    /**
     * 边的起始节点
     *
     * v -> e
     *
     * @return
     */
    GraphVertex inVertex();

    /**
     * 边关联的全部节点
     *
     * @return
     */
   Iterator<GraphVertex> bothVertices();

    /**
     * 边属性
     *
     * @param propertyKeys
     * @param <V>
     * @return
     */
    @Override
    <V> Iterator<GraphProperty<V>> properties(final String... propertyKeys);

}
