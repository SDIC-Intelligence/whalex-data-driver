package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.*;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.*;

/**
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class GremlinResult implements GraphResult {

    private Result result;

    public GremlinResult(Result result) {
        this.result = result;
    }

    @Override
    public String getString() {
        return this.result.getString();
    }

    @Override
    public int getInt() {
        return this.result.getInt();
    }

    @Override
    public byte getByte() {
        return this.result.getByte();
    }

    @Override
    public short getShort() {
        return this.result.getShort();
    }

    @Override
    public long getLong() {
        return this.result.getLong();
    }

    @Override
    public float getFloat() {
        return this.result.getFloat();
    }

    @Override
    public double getDouble() {
        return this.result.getDouble();
    }

    @Override
    public boolean getBoolean() {
        return this.result.getBoolean();
    }

    @Override
    public boolean isNull() {
        return this.result.isNull();
    }

    @Override
    public GraphVertex getVertex() {
        Vertex vertex = this.result.getVertex();
        return new GremlinVertex(vertex);
    }

    @Override
    public GraphEdge getEdge() {
        Edge edge = this.result.getEdge();
        return new GremlinEdge(edge);
    }

    @Override
    public GraphElement getElement() {
        Element element = this.result.getElement();
        return new GremlinElement(element);
    }

    @Override
    public GraphPath getPath() {
        Path path = this.result.getPath();
        return new GremlinPath(path);
    }

    @Override
    public <V> GraphProperty<V> getProperty() {
        return new GremlinProperty<>(this.result.getProperty());
    }

    @Override
    public <V> GraphVertexProperty<V> getVertexProperty() {
        return new GremlinVertexProperty<>(this.result.getVertexProperty());
    }

    @Override
    public Object getObject() {
        if (this.result.getObject() instanceof Vertex) {
            return this.getVertex();
        } else if (this.result.getObject() instanceof Edge) {
            return this.getEdge();
        } else if (this.result.getObject() instanceof Path) {
            return this.getPath();
        } else if (this.result.getObject() instanceof Element) {
            return this.getElement();
        } else if (this.result.getObject() instanceof Property) {
            return this.result.getProperty();
        } else if (this.result.getObject() instanceof VertexProperty) {
            return this.result.getVertexProperty();
        } else {
            return this.result.getObject();
        }
    }

    @Override
    public <T> T get(final Class<? extends T> clazz) {
        return this.result.get(clazz);
    }

    @Override
    public String toString() {
        String c = this.getObject() != null ? this.getObject().getClass().getCanonicalName() : "null";
        return "result{object=" + this.getObject() + " class=" + c + '}';
    }
}
