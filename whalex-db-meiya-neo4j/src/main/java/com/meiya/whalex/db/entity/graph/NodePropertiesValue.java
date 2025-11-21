package com.meiya.whalex.db.entity.graph;

import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.internal.value.ObjectValueAdapter;
import org.neo4j.driver.types.Type;

/**
 * @author 黄河森
 * @date 2023/3/31
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class NodePropertiesValue<V> extends ObjectValueAdapter<NodeProperties<V>> {
    protected NodePropertiesValue(NodeProperties<V> adapted) {
        super(adapted);
    }

    public NodeProperties<V> asProperties() {
        return this.asObject();
    }

    @Override
    public Type type() {
        return new TypeRepresentation(TypeConstructor.NODE);
    }
}
