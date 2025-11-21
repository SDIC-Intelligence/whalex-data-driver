package com.meiya.whalex.db.entity.graph;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.util.param.impl.graph.ReturnType;
import com.meiya.whalex.graph.entity.GraphEdge;
import com.meiya.whalex.graph.entity.GraphElement;
import com.meiya.whalex.graph.entity.GraphPath;
import com.meiya.whalex.graph.entity.GraphVertex;
import com.meiya.whalex.graph.exception.GremlinParserException;
import com.meiya.whalex.graph.util.GraphStringFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.*;

/**
 * @author 黄河森
 * @date 2023/3/24
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class Neo4jPath implements GraphPath {

    private List<Object> objects = new ArrayList<>();

    private List<Set<String>> labels;

    private InternalPath path;

    private ReturnType.PATH returnType;

    public Neo4jPath(InternalPath path, ReturnType.PATH returnType) {
        this.path = path;
        this.returnType = returnType;
        List<ReturnType> _returnType = this.returnType.getReturnType();
        Iterator<InternalPath.Segment> iterator = this.path.iterator();
        List<GraphElement> entityList = new ArrayList<>();
        while (iterator.hasNext()) {
            InternalPath.Segment segment = iterator.next();
            Node start = segment.start();
            Relationship relationship = segment.relationship();
            Node end = segment.end();
            if (CollectionUtil.isEmpty(entityList)) {
                // Neo4J 返回 Segment 内容中，每一段的 startNode 会衔接上一段的 endNode，所以只有在第一次遍历时加入 startNode
                entityList.add(new Neo4jVertex((InternalNode) start));
            }
            entityList.add(new Neo4jEdge((InternalRelationship) relationship, (InternalNode) start, (InternalNode) end));
            entityList.add(new Neo4jVertex((InternalNode) end));
        }

        Iterator<GraphElement> entityIterator = entityList.iterator();
        for (ReturnType type : _returnType) {
            if (type instanceof ReturnType.E) {
                while (entityIterator.hasNext()) {
                    GraphElement next = entityIterator.next();
                    if (next instanceof Neo4jEdge) {
                        this.objects.add(next);
                        break;
                    }
                }
            } else {
                while (entityIterator.hasNext()) {
                    GraphElement next = entityIterator.next();
                    if (next instanceof Neo4jVertex) {
                        this.objects.add(next);
                        break;
                    }
                }
            }
        }
        ReturnType type = _returnType.get(_returnType.size() - 1);
        while (entityIterator.hasNext()) {
            GraphElement next = entityIterator.next();
            if (type instanceof ReturnType.V && next instanceof GraphVertex) {
                this.objects.add(next);
            } else if (type instanceof ReturnType.E && next instanceof GraphEdge) {
                this.objects.add(next);
            }
        }
    }

    @Override
    public List<Object> objects() {
        return objects;
    }

    @Override
    public List<Set<String>> labels() {
        return labels;
    }

    @Override
    public String toString() {
        return GraphStringFactory.pathString(this);
    }

    @Override
    public int hashCode() {
        return objects().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof Path)) {
            return false;
        } else {
            Path otherPath = (Path)other;
            if (otherPath.size() != this.objects().size()) {
                return false;
            } else {
                List<Object> otherPathObjects = otherPath.objects();
                List<Set<String>> otherPathLabels = otherPath.labels();

                for(int i = this.objects().size() - 1; i >= 0; --i) {
                    if (!this.objects().get(i).equals(otherPathObjects.get(i))) {
                        return false;
                    }

                    if (!((Set)this.labels().get(i)).equals(otherPathLabels.get(i))) {
                        return false;
                    }
                }

                return true;
            }
        }
    }
}
