package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.*;
import com.meiya.whalex.graph.util.iterator.GraphIteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Iterator;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class GremlinElement<V extends Element> implements GraphElement {

    protected V element;

    public GremlinElement(V element) {
        this.element = element;
    }

    @Override
    public Object id() {
        return this.element.id();
    }

    @Override
    public String label() {
        return this.element.label();
    }

    @Override
    public <V> Iterator<? extends GraphProperty<V>> properties(String... propertyKeys) {
        return GraphIteratorUtils.<Property<V>, GraphProperty<V>>map((Iterator<Property<V>>) this.element.<V>properties(propertyKeys), (Function<Property<V>, GraphProperty<V>>) vProperty -> new GremlinProperty<V>(vProperty));
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
