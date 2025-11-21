package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.GraphDirection;
import com.meiya.whalex.graph.entity.GraphEdge;
import com.meiya.whalex.graph.entity.GraphProperty;
import com.meiya.whalex.graph.entity.GraphVertex;
import com.meiya.whalex.graph.util.GraphStringFactory;
import com.meiya.whalex.graph.util.iterator.GraphIteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;

import java.util.Iterator;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2023/3/24
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class Neo4jEdge extends Neo4jElement<InternalRelationship> implements GraphEdge {

    private InternalNode startNode;

    private InternalNode endNode;

    public Neo4jEdge(InternalRelationship element, InternalNode startNode, InternalNode endNode) {
        super(element);
        this.startNode = startNode;
        this.endNode = endNode;
    }

    @Override
    public Iterator<GraphVertex> vertices(GraphDirection direction) {
        switch (direction) {
            case IN:
                return GraphIteratorUtils.of(inVertex());
            case OUT:
                return GraphIteratorUtils.of(outVertex());
            case BOTH:
                return bothVertices();
        }
        return null;
    }

    @Override
    public GraphVertex outVertex() {
        Neo4jVertex neo4jVertex = new Neo4jVertex(endNode);
        return neo4jVertex;
    }

    @Override
    public GraphVertex inVertex() {
        Neo4jVertex neo4jVertex = new Neo4jVertex(startNode);
        return neo4jVertex;
    }

    @Override
    public Iterator<GraphVertex> bothVertices() {
        return GraphIteratorUtils.of(new Neo4jVertex(startNode), new Neo4jVertex(endNode));
    }

    @Override
    public Object id() {
        return String.valueOf(this.element.id());
    }

    @Override
    public String label() {
        return this.element.type();
    }

    @Override
    public <V> Iterator<GraphProperty<V>> properties(String... propertyKeys) {
        return (Iterator<GraphProperty<V>>) super.<V>properties(propertyKeys);
    }

    @Override
    public String toString() {
        return GraphStringFactory.edgeString(this);
    }
}
