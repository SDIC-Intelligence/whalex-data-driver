package com.meiya.whalex.graph.entity;

import java.util.Objects;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlInternalPoint2D
 */
public class QlInternalPoint2D implements QlPoint {

    private final int srid;
    private final double x;
    private final double y;

    public QlInternalPoint2D(int srid, double x, double y) {
        this.srid = srid;
        this.x = x;
        this.y = y;
    }

    public int srid() {
        return this.srid;
    }

    public double x() {
        return this.x;
    }

    public double y() {
        return this.y;
    }

    public double z() {
        return Double.NaN;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlInternalPoint2D that = (QlInternalPoint2D)o;
            return this.srid == that.srid && Double.compare(that.x, this.x) == 0 && Double.compare(that.y, this.y) == 0;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.srid, this.x, this.y});
    }

    public String toString() {
        return "Point{srid=" + this.srid + ", x=" + this.x + ", y=" + this.y + '}';
    }

}
