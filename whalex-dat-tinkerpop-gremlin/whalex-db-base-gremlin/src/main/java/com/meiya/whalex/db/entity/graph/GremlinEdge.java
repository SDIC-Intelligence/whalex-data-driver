package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.GraphDirection;
import com.meiya.whalex.graph.entity.GraphEdge;
import com.meiya.whalex.graph.entity.GraphProperty;
import com.meiya.whalex.graph.entity.GraphVertex;
import com.meiya.whalex.graph.util.GraphStringFactory;
import com.meiya.whalex.graph.util.iterator.GraphIteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class GremlinEdge extends GremlinElement<Edge> implements GraphEdge {

    public GremlinEdge(Edge edge) {
        super(edge);
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
    public Iterator<GraphVertex> vertices(GraphDirection direction) {
        Iterator<Vertex> vertices = element.vertices(Direction.valueOf(direction.name()));
        return GraphIteratorUtils.map(vertices, new Function<Vertex, GraphVertex>() {
            @Override
            public GraphVertex apply(Vertex vertex) {
                return new GremlinVertex(vertex);
            }
        });
    }

    @Override
    public GraphVertex outVertex() {
        return this.vertices(GraphDirection.OUT).next();
    }

    @Override
    public GraphVertex inVertex() {
        return this.vertices(GraphDirection.IN).next();
    }

    @Override
    public Iterator<GraphVertex> bothVertices() {
        return this.vertices(GraphDirection.BOTH);
    }

    @Override
    public <V> Iterator<GraphProperty<V>> properties(String... propertyKeys) {
        return GraphIteratorUtils.map(element.properties(propertyKeys), new Function<Property<V>, GraphProperty<V>>() {
            @Override
            public GraphProperty<V> apply(Property<V> vProperty) {
                return new GremlinProperty(vProperty);
            }
        });
    }

    @Override
    public String toString() {
        return GraphStringFactory.edgeString(this);
    }
}
