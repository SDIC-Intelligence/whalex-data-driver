package com.meiya.whalex.db.entity.graph;

import org.neo4j.driver.types.Node;

/**
 * 节点属性对象
 * gremlin .properties() 返回
 *
 * @author 黄河森
 * @date 2023/3/31
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public interface NodeProperties<V> {

    /**
     * 属性对应节点
     *
     * @return
     */
    Node node();

    /**
     * 属性key
     *
     * @return
     */
    String key();

    /**
     * 属性值
     *
     * @return
     */
    V value();

}
