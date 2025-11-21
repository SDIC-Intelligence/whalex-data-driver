package com.meiya.whalex.graph.entity;

import java.util.*;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description InternalQlPath
 */
public class InternalQlPath implements QlPath, QlAsValue {

    private final List<QlNode> nodes;
    private final List<QlRelationship> relationships;
    private final List<Segment> segments;

    private static boolean isEndpoint(QlNode node, QlRelationship relationship) {
        return node.id() == relationship.startNodeId() || node.id() == relationship.endNodeId();
    }

    public InternalQlPath(List<QlEntity> alternatingNodeAndRel) {
        this.nodes = this.newList(alternatingNodeAndRel.size() / 2 + 1);
        this.relationships = this.newList(alternatingNodeAndRel.size() / 2);
        this.segments = this.newList(alternatingNodeAndRel.size() / 2);
        if (alternatingNodeAndRel.size() % 2 == 0) {
            throw new IllegalArgumentException("An odd number of entities are required to build a path");
        } else {
            QlNode lastNode = null;
            QlRelationship lastRelationship = null;
            int index = 0;

            for(Iterator var5 = alternatingNodeAndRel.iterator(); var5.hasNext(); ++index) {
                QlEntity entity = (QlEntity)var5.next();
                if (entity == null) {
                    throw new IllegalArgumentException("Path entities cannot be null");
                }

                String cls;
                if (index % 2 == 0) {
                    try {
                        lastNode = (QlNode)entity;
                        if (!this.nodes.isEmpty() && !isEndpoint(lastNode, lastRelationship)) {
                            throw new IllegalArgumentException("Node argument " + index + " is not an endpoint of relationship argument " + (index - 1));
                        }

                        this.nodes.add(lastNode);
                    } catch (ClassCastException var9) {
                        cls = entity.getClass().getName();
                        throw new IllegalArgumentException("Expected argument " + index + " to be a node " + index + " but found a " + cls + " instead");
                    }
                } else {
                    try {
                        lastRelationship = (QlRelationship)entity;
                        if (!isEndpoint(lastNode, lastRelationship)) {
                            throw new IllegalArgumentException("Node argument " + (index - 1) + " is not an endpoint of relationship argument " + index);
                        }

                        this.relationships.add(lastRelationship);
                    } catch (ClassCastException var10) {
                        cls = entity.getClass().getName();
                        throw new IllegalArgumentException("Expected argument " + index + " to be a relationship but found a " + cls + " instead");
                    }
                }
            }

            this.buildSegments();
        }
    }

    public InternalQlPath(QlEntity... alternatingNodeAndRel) {
        this(Arrays.asList(alternatingNodeAndRel));
    }

    public InternalQlPath(List<Segment> segments, List<QlNode> nodes, List<QlRelationship> relationships) {
        this.segments = segments;
        this.nodes = nodes;
        this.relationships = relationships;
    }

    private <T> List<T> newList(int size) {
        return (List)(size == 0 ? Collections.emptyList() : new ArrayList(size));
    }

    public int length() {
        return this.relationships.size();
    }

    public boolean contains(QlNode node) {
        return this.nodes.contains(node);
    }

    public boolean contains(QlRelationship relationship) {
        return this.relationships.contains(relationship);
    }

    public Iterable<QlNode> nodes() {
        return this.nodes;
    }

    public Iterable<QlRelationship> relationships() {
        return this.relationships;
    }

    public QlNode start() {
        return (QlNode)this.nodes.get(0);
    }

    public QlNode end() {
        return (QlNode)this.nodes.get(this.nodes.size() - 1);
    }

    public Iterator<Segment> iterator() {
        return this.segments.iterator();
    }

    public QlValue asValue() {
        return new QlPathValue(this);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            InternalQlPath segments1 = (InternalQlPath)o;
            return this.segments.equals(segments1.segments);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return this.segments.hashCode();
    }

    public String toString() {
        return "path" + this.segments;
    }

    private void buildSegments() {
        for(int i = 0; i < this.relationships.size(); ++i) {
            this.segments.add(new SelfContainedSegment((QlNode)this.nodes.get(i), (QlRelationship)this.relationships.get(i), (QlNode)this.nodes.get(i + 1)));
        }

    }

    public static class SelfContainedSegment implements Segment {
        private final QlNode start;
        private final QlRelationship relationship;
        private final QlNode end;

        public SelfContainedSegment(QlNode start, QlRelationship relationship, QlNode end) {
            this.start = start;
            this.relationship = relationship;
            this.end = end;
        }

        public QlNode start() {
            return this.start;
        }

        public QlRelationship relationship() {
            return this.relationship;
        }

        public QlNode end() {
            return this.end;
        }

        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other != null && this.getClass() == other.getClass()) {
                SelfContainedSegment that = (SelfContainedSegment)other;
                return this.start.equals(that.start) && this.end.equals(that.end) && this.relationship.equals(that.relationship);
            } else {
                return false;
            }
        }

        public int hashCode() {
            int result = this.start.hashCode();
            result = 31 * result + this.relationship.hashCode();
            result = 31 * result + this.end.hashCode();
            return result;
        }

        public String toString() {
            return String.format(this.relationship.startNodeId() == this.start.id() ? "(%s)-[%s:%s]->(%s)" : "(%s)<-[%s:%s]-(%s)", this.start.id(), this.relationship.id(), this.relationship.type(), this.end.id());
        }
    }

}
