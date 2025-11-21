package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.util.iterator.QlIterables;

import java.util.Map;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description InternalQlEntity
 */
public class InternalQlEntity implements QlEntity, QlAsValue {

    private final long id;
    private final Map<String, QlValue> properties;

    public InternalQlEntity(long id, Map<String, QlValue> properties) {
        this.id = id;
        this.properties = properties;
    }

    public long id() {
        return this.id;
    }

    public int size() {
        return this.properties.size();
    }

    public Map<String, Object> asMap() {
        return this.asMap(QlValues.ofObject());
    }

    public <T> Map<String, T> asMap(Function<QlValue, T> mapFunction) {
        return QlExtract.map(this.properties, mapFunction);
    }

    public QlValue asValue() {
        return new QlMapValue(this.properties);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            InternalQlEntity that = (InternalQlEntity)o;
            return this.id == that.id;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return (int)(this.id ^ this.id >>> 32);
    }

    public String toString() {
        return "Entity{id=" + this.id + ", properties=" + this.properties + '}';
    }

    public boolean containsKey(String key) {
        return this.properties.containsKey(key);
    }

    public Iterable<String> keys() {
        return this.properties.keySet();
    }

    public QlValue get(String key) {
        QlValue value = (QlValue)this.properties.get(key);
        return value == null ? QlValues.NULL : value;
    }

    public Iterable<QlValue> values() {
        return this.properties.values();
    }

    public <T> Iterable<T> values(Function<QlValue, T> mapFunction) {
        return QlIterables.map(this.properties.values(), mapFunction);
    }

}
