package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.exception.QlLossyCoercion;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlFloatValue
 */
public class QlFloatValue extends QlNumberValueAdapter<Double> {

    private final double val;

    public QlFloatValue(double val) {
        this.val = val;
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.FLOAT();
    }

    public Double asNumber() {
        return this.val;
    }

    public long asLong() {
        long longVal = (long)this.val;
        if ((double)longVal != this.val) {
            throw new QlLossyCoercion(this.type().name(), "Java long");
        } else {
            return longVal;
        }
    }

    public int asInt() {
        int intVal = (int)this.val;
        if ((double)intVal != this.val) {
            throw new QlLossyCoercion(this.type().name(), "Java int");
        } else {
            return intVal;
        }
    }

    public double asDouble() {
        return this.val;
    }

    public float asFloat() {
        float floatVal = (float)this.val;
        if ((double)floatVal != this.val) {
            throw new QlLossyCoercion(this.type().name(), "Java float");
        } else {
            return floatVal;
        }
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlFloatValue values = (QlFloatValue)o;
            return Double.compare(values.val, this.val) == 0;
        } else {
            return false;
        }
    }

    public int hashCode() {
        long temp = Double.doubleToLongBits(this.val);
        return (int)(temp ^ temp >>> 32);
    }

    public String toString() {
        return Double.toString(this.val);
    }

}
