package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.GraphDirection;
import com.meiya.whalex.graph.entity.GraphEdge;
import com.meiya.whalex.graph.entity.GraphVertex;
import com.meiya.whalex.graph.entity.GraphVertexProperty;
import com.meiya.whalex.graph.util.GraphStringFactory;
import com.meiya.whalex.graph.util.iterator.GraphIteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class GremlinVertex extends GremlinElement<Vertex> implements GraphVertex {

    public GremlinVertex(Vertex vertex) {
        super(vertex);
    }

    @Override
    public <V> GraphVertexProperty<V> property(GraphVertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        VertexProperty<V> property = element.property(VertexProperty.Cardinality.valueOf(cardinality.name()), key, value, keyValues);
        return new GremlinVertexProperty<>(property);
    }

    @Override
    public Iterator<GraphEdge> edges(GraphDirection direction, String... edgeLabels) {
        return GraphIteratorUtils.map(element.edges(Direction.valueOf(direction.name()), edgeLabels), new Function<Edge, GraphEdge>() {
            @Override
            public GraphEdge apply(Edge edge) {
                return new GremlinEdge(edge);
            }
        });
    }

    @Override
    public Iterator<GraphVertex> vertices(GraphDirection direction, String... edgeLabels) {
        return GraphIteratorUtils.map(element.vertices(Direction.valueOf(direction.name()), edgeLabels), new Function<Vertex, GraphVertex>() {
            @Override
            public GraphVertex apply(Vertex vertex) {
                return new GremlinVertex(vertex);
            }
        });
    }

    @Override
    public Object id() {
        return element.id();
    }

    @Override
    public String label() {
        return element.label();
    }

    @Override
    public <V> Iterator<GraphVertexProperty<V>> properties(String... propertyKeys) {
        return GraphIteratorUtils.map(element.properties(propertyKeys), new Function<VertexProperty<V>, GraphVertexProperty<V>>() {
            @Override
            public GraphVertexProperty<V> apply(VertexProperty<V> objectVertexProperty) {
                return new GremlinVertexProperty<V>(objectVertexProperty);
            }
        });
    }

    @Override
    public String toString() {
        return GraphStringFactory.vertexString(this);
    }
}
