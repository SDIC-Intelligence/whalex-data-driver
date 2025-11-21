package com.meiya.whalex.db.entity.graph;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ArrayUtil;
import com.meiya.whalex.graph.entity.GraphProperty;
import com.meiya.whalex.graph.entity.GraphVertex;
import com.meiya.whalex.graph.entity.GraphVertexProperty;
import com.meiya.whalex.graph.util.iterator.GraphIteratorUtils;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.types.Node;

import java.util.*;

/**
 * @author 黄河森
 * @date 2023/3/24
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class Neo4jVertexProperty<V> implements GraphVertexProperty<V> {

    private InternalNodeProperties<V> properties;

    public Neo4jVertexProperty(Map.Entry<String, Value> entry, InternalNode vertex) {
        this.properties = new InternalNodeProperties<V>(vertex, entry.getKey(), entry.getValue());
    }

    public Neo4jVertexProperty(InternalNodeProperties<V> properties) {
        this.properties = properties;
    }

    @Override
    public String key() {
        return properties.key();
    }

    @Override
    public V value() throws NoSuchElementException {
        return properties.value();
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public GraphVertex element() {
        Node node = properties.node();
        return new Neo4jVertex((InternalNode) node);
    }

    @Override
    public Object id() {
        return properties.key();
    }

    @Override
    public <U> Iterator<GraphProperty<U>> properties(String... propertyKeys) {
        Iterator<GraphProperty<U>> of;
        if (ArrayUtil.isNotEmpty(propertyKeys)) {
            ArrayList<String> propertyKeyList = CollectionUtil.newArrayList(propertyKeys);
            if (propertyKeyList.contains(properties.key())) {
                of = GraphIteratorUtils.of((GraphProperty) this);
            } else {
                of = Collections.emptyIterator();
            }
        } else {
            of = GraphIteratorUtils.of((GraphProperty) this);
        }
        return of;
    }
}
