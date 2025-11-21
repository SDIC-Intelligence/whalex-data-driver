package com.meiya.whalex.graph.entity;

import java.util.Objects;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlObjectValueAdapter
 */
public abstract class QlObjectValueAdapter<V> extends QlValueAdapter {

    private final V adapted;

    protected QlObjectValueAdapter(V adapted) {
        if (adapted == null) {
            throw new IllegalArgumentException(String.format("Cannot construct %s from null", this.getClass().getSimpleName()));
        } else {
            this.adapted = adapted;
        }
    }

    public final V asObject() {
        return this.adapted;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlObjectValueAdapter<?> that = (QlObjectValueAdapter)o;
            return Objects.equals(this.adapted, that.adapted);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return this.adapted.hashCode();
    }

    public String toString() {
        return this.adapted.toString();
    }

}
