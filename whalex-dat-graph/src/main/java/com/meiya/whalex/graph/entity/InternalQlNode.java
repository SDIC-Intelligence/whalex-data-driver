package com.meiya.whalex.graph.entity;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description InternalQlNode
 */
public class InternalQlNode extends InternalQlEntity implements QlNode {

    private final Collection<String> labels;

    public InternalQlNode(long id) {
        this(id, Collections.emptyList(), Collections.emptyMap());
    }

    public InternalQlNode(long id, Collection<String> labels, Map<String, QlValue> properties) {
        super(id, properties);
        this.labels = labels;
    }

    public Collection<String> labels() {
        return this.labels;
    }

    public boolean hasLabel(String label) {
        return this.labels.contains(label);
    }

    public QlValue asValue() {
        return new QlNodeValue(this);
    }

    public String toString() {
        return String.format("node<%s>", this.id());
    }

}
