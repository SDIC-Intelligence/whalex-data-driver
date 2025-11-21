package com.meiya.whalex.graph.entity;

import java.util.Collections;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description InternalQlRelationship
 */
public class InternalQlRelationship extends InternalQlEntity implements QlRelationship {

    private long start;
    private long end;
    private final String type;

    public InternalQlRelationship(long id, long start, long end, String type) {
        this(id, start, end, type, Collections.emptyMap());
    }

    public InternalQlRelationship(long id, long start, long end, String type, Map<String, QlValue> properties) {
        super(id, properties);
        this.start = start;
        this.end = end;
        this.type = type;
    }

    public boolean hasType(String relationshipType) {
        return this.type().equals(relationshipType);
    }

    public void setStartAndEnd(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long startNodeId() {
        return this.start;
    }

    public long endNodeId() {
        return this.end;
    }

    public String type() {
        return this.type;
    }

    public QlValue asValue() {
        return new QlRelationshipValue(this);
    }

    public String toString() {
        return String.format("relationship<%s>", this.id());
    }

}
