package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description NullQlValue
 */
public final class QlNullValue extends QlValueAdapter {

    public static final QlValue NULL = new QlNullValue();

    private QlNullValue() {
    }

    public boolean isNull() {
        return true;
    }

    public Object asObject() {
        return null;
    }

    public String asString() {
        return "null";
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.NULL();
    }

    public boolean equals(Object obj) {
        return obj == NULL;
    }

    public int hashCode() {
        return 0;
    }

    public String toString() {
        return "NULL";
    }

}
