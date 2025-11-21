package com.meiya.whalex.graph.entity;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlListValue
 */
public class QlListValue extends QlValueAdapter {
    private final QlValue[] values;

    public QlListValue(QlValue... values) {
        if (values == null) {
            throw new IllegalArgumentException("Cannot construct QlListValue from null");
        } else {
            this.values = values;
        }
    }

    public boolean isEmpty() {
        return this.values.length == 0;
    }

    public List<Object> asObject() {
        return this.asList(QlValues.ofObject());
    }

    public List<Object> asList() {
        return QlExtract.list(this.values, QlValues.ofObject());
    }

    public <T> List<T> asList(Function<QlValue, T> mapFunction) {
        return QlExtract.list(this.values, mapFunction);
    }

    public int size() {
        return this.values.length;
    }

    public QlValue get(int index) {
        return index >= 0 && index < this.values.length ? this.values[index] : QlValues.NULL;
    }

    public <T> Iterable<T> values(final Function<QlValue, T> mapFunction) {
        return new Iterable<T>() {
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    private int cursor = 0;

                    public boolean hasNext() {
                        return this.cursor < QlListValue.this.values.length;
                    }

                    public T next() {
                        return mapFunction.apply(QlListValue.this.values[this.cursor++]);
                    }

                    public void remove() {
                    }
                };
            }
        };
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.LIST();
    }

    public String toString() {
        return Arrays.toString(this.values);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlListValue otherValues = (QlListValue)o;
            return Arrays.equals(this.values, otherValues.values);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Arrays.hashCode(this.values);
    }
}
