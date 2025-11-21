package com.meiya.whalex.db.entity.graph;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ObjectUtil;
import com.meiya.whalex.graph.entity.*;
import com.meiya.whalex.graph.exception.GremlinParserException;
import com.meiya.whalex.graph.util.iterator.GraphIteratorUtils;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.types.Entity;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author 黄河森
 * @date 2023/3/24
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class Neo4jElement<E extends Entity> implements GraphElement {

    protected E element;

    public Neo4jElement(E element) {
        this.element = element;
    }

    @Override
    public Object id() {
        return element.id();
    }

    @Override
    public String label() {
        if (element instanceof InternalNode) {
            InternalNode node = (InternalNode) element;
            Collection<String> labels = node.labels();
            if (CollectionUtil.isNotEmpty(labels)) {
                return labels.iterator().next();
            } else {
                return null;
            }
        } else if (element instanceof InternalRelationship) {
            InternalRelationship internalRelationship = (InternalRelationship) element;
            return internalRelationship.type();
        } else {
            throw new GremlinParserException("无法解析当前对象类型： [" + element.getClass().getName() + "]");
        }
    }

    @Override
    public <V> Iterator<? extends GraphProperty<V>> properties(String... propertyKeys) {
        List<String> propertyKeyList = CollectionUtil.newArrayList(propertyKeys);
        Set<Map.Entry<String, V>> entries = this.element.asMap(new Function<Value, V>() {
            @Override
            public V apply(Value value) {
                return (V) value.asObject();
            }
        }).entrySet();
        if (CollectionUtil.isNotEmpty(propertyKeyList)) {
            entries = entries.stream().filter((entry) -> propertyKeyList.contains(entry.getKey())).collect(Collectors.toSet());
        }
        return GraphIteratorUtils.<Map.Entry<String, V>, GraphProperty<V>>map(entries.iterator(), vProperty -> new Neo4jProperty<V>(vProperty.getKey(), vProperty.getValue()));
    }

    @Override
    public int hashCode() {
        return id().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        if (null != other) {
            if (this == other) {
                return true;
            } else {
                return this instanceof GraphVertex && other instanceof GraphVertex || this instanceof GraphEdge && other instanceof GraphEdge
                        || this instanceof GraphVertexProperty && other instanceof GraphVertexProperty ? (this.id().equals(((GraphElement)other).id())) : false;
            }
        } else {
            return false;
        }
    }
}
