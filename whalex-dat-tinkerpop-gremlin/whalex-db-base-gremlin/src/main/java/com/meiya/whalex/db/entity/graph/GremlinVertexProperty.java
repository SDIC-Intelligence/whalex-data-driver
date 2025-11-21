package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.GraphProperty;
import com.meiya.whalex.graph.entity.GraphVertex;
import com.meiya.whalex.graph.entity.GraphVertexProperty;
import com.meiya.whalex.graph.util.iterator.GraphIteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class GremlinVertexProperty<V> implements GraphVertexProperty<V> {

    private VertexProperty<V> vertexProperty;

    public GremlinVertexProperty(VertexProperty<V> vertexProperty) {
        this.vertexProperty = vertexProperty;
    }

    @Override
    public String key() {
        return vertexProperty.key();
    }

    @Override
    public V value() throws NoSuchElementException {
        return vertexProperty.value();
    }

    @Override
    public boolean isPresent() {
        return vertexProperty.isPresent();
    }

    @Override
    public GraphVertex element() {
        Vertex element = this.vertexProperty.element();
        return new GremlinVertex(element);
    }

    @Override
    public Iterator<GraphProperty> properties(String... propertyKeys) {
        return GraphIteratorUtils.map(this.vertexProperty.properties(propertyKeys), new Function<Property<Object>, GraphProperty>() {
            @Override
            public GraphProperty apply(Property<Object> objectProperty) {
                return new GremlinProperty(objectProperty);
            }
        });
    }

    @Override
    public Object id() {
        return this.vertexProperty.id();
    }
}
