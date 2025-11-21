package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.GraphElement;
import com.meiya.whalex.graph.entity.GraphProperty;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.NoSuchElementException;

/**
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class GremlinProperty<V> implements GraphProperty<V> {

    private Property<V> property;

    public GremlinProperty(Property<V> property) {
        this.property = property;
    }

    @Override
    public String key() {
        return this.property.key();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.property.value();
    }

    @Override
    public boolean isPresent() {
        return this.property.isPresent();
    }

    @Override
    public GraphElement element() {
        Element element = this.property.element();
        return new GremlinElement(element);
    }
}
