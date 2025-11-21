package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.exception.QlLossyCoercion;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlIntegerValue
 */
public class QlIntegerValue extends QlNumberValueAdapter<Long> {

    private final long val;

    public QlIntegerValue(long val) {
        this.val = val;
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.INTEGER();
    }

    public Long asNumber() {
        return this.val;
    }

    public long asLong() {
        return this.val;
    }

    public int asInt() {
        if (this.val <= 2147483647L && this.val >= -2147483648L) {
            return (int)this.val;
        } else {
            throw new QlLossyCoercion(this.type().name(), "Java int");
        }
    }

    public double asDouble() {
        double doubleVal = (double)this.val;
        if ((long)doubleVal != this.val) {
            throw new QlLossyCoercion(this.type().name(), "Java double");
        } else {
            return (double)this.val;
        }
    }

    public float asFloat() {
        return (float)this.val;
    }

    public String toString() {
        return Long.toString(this.val);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlIntegerValue values = (QlIntegerValue)o;
            return this.val == values.val;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return (int)(this.val ^ this.val >>> 32);
    }

}
