package com.meiya.whalex.db.entity.graph;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.graph.entity.GraphDirection;
import com.meiya.whalex.graph.entity.GraphEdge;
import com.meiya.whalex.graph.entity.GraphVertex;
import com.meiya.whalex.graph.entity.GraphVertexProperty;
import com.meiya.whalex.graph.exception.GremlinNotImplementedException;
import com.meiya.whalex.graph.util.GraphStringFactory;
import com.meiya.whalex.graph.util.iterator.GraphIteratorUtils;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2023/3/24
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class Neo4jVertex extends Neo4jElement<InternalNode> implements GraphVertex {

    public Neo4jVertex(InternalNode element) {
        super(element);
    }

    @Override
    public <V> GraphVertexProperty<V> property(GraphVertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        throw new GremlinNotImplementedException("Neo4j GraphVertex 未实现节点新增属性方法!");
    }

    @Override
    public Iterator<GraphEdge> edges(GraphDirection direction, String... edgeLabels) {
        throw new GremlinNotImplementedException("Neo4j GraphVertex 未实现节点探寻边的方法!");
    }

    @Override
    public Iterator<GraphVertex> vertices(GraphDirection direction, String... edgeLabels) {
        throw new GremlinNotImplementedException("Neo4j GraphVertex 未实现节点探寻节点的方法!");
    }

    @Override
    public Object id() {
        return String.valueOf(this.element.id());
    }

    @Override
    public String label() {
        Collection<String> labels = this.element.labels();
        if (CollectionUtil.isNotEmpty(labels)) {
            return labels.iterator().next();
        } else {
            return null;
        }
    }

    @Override
    public <V> Iterator<GraphVertexProperty<V>> properties(String... propertyKeys) {
        Map<String, Value> map = this.element.asMap(new Function<Value, Value>() {
            @Override
            public Value apply(Value value) {
                return value;
            }
        });
        return GraphIteratorUtils.map(map.entrySet().iterator(), new Function<Map.Entry<String, Value>, GraphVertexProperty<V>>() {
            @Override
            public GraphVertexProperty<V> apply(Map.Entry<String, Value> stringObjectEntry) {
                return new Neo4jVertexProperty<V>(stringObjectEntry, Neo4jVertex.this.element);
            }
        });
    }

    @Override
    public String toString() {
        return GraphStringFactory.vertexString(this);
    }
}
