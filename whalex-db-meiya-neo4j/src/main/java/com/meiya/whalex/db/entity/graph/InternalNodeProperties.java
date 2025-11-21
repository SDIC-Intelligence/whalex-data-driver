package com.meiya.whalex.db.entity.graph;

import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;

/**
 * @author 黄河森
 * @date 2023/3/31
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class InternalNodeProperties<V> implements NodeProperties<V> {

    private final Node node;

    private final String key;

    private final Value value;

    public InternalNodeProperties(Node node, String key, Value value) {
        this.node = node;
        this.key = key;
        this.value = value;
    }

    @Override
    public Node node() {
        return node;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() {
        return (V) value.asObject();
    }
}
