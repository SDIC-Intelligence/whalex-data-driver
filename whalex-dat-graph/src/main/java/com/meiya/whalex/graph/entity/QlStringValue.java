package com.meiya.whalex.graph.entity;

import java.util.Objects;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlStringValue
 */
public class QlStringValue extends QlValueAdapter {

    private final String val;

    public QlStringValue(String val) {
        if (val == null) {
            throw new IllegalArgumentException("Cannot construct StringValue from null");
        } else {
            this.val = val;
        }
    }

    public boolean isEmpty() {
        return this.val.isEmpty();
    }

    public int size() {
        return this.val.length();
    }

    public String asObject() {
        return this.asString();
    }

    public String asString() {
        return this.val;
    }

    public String toString() {
        return String.format("\"%s\"", this.val.replace("\"", "\\\""));
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.STRING();
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlStringValue that = (QlStringValue)o;
            return Objects.equals(this.val, that.val);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return this.val.hashCode();
    }

}
