package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.util.QlFormat;

import java.util.Map;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description MapValue
 */
public class QlMapValue extends QlValueAdapter {

    private final Map<String, QlValue> val;

    public QlMapValue(Map<String, QlValue> val) {
        if (val == null) {
            throw new IllegalArgumentException("Cannot construct MapValue from null");
        } else {
            this.val = val;
        }
    }

    public boolean isEmpty() {
        return this.val.isEmpty();
    }

    public Map<String, Object> asObject() {
        return this.asMap(QlValues.ofObject());
    }

    public Map<String, Object> asMap() {
        return QlExtract.map(this.val, QlValues.ofObject());
    }

    public <T> Map<String, T> asMap(Function<QlValue, T> mapFunction) {
        return QlExtract.map(this.val, mapFunction);
    }

    public int size() {
        return this.val.size();
    }

    public boolean containsKey(String key) {
        return this.val.containsKey(key);
    }

    public Iterable<String> keys() {
        return this.val.keySet();
    }

    public Iterable<QlValue> values() {
        return this.val.values();
    }

    public <T> Iterable<T> values(Function<QlValue, T> mapFunction) {
        return QlExtract.map(this.val, mapFunction).values();
    }

    public QlValue get(String key) {
        QlValue value = (QlValue)this.val.get(key);
        return value == null ? QlValues.NULL : value;
    }

    public String toString() {
        return QlFormat.formatPairs(this.asMap(QlValues.ofValue()));
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.MAP();
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlMapValue values = (QlMapValue)o;
            return this.val.equals(values.val);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return this.val.hashCode();
    }

}
