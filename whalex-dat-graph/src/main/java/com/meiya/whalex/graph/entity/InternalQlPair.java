package com.meiya.whalex.graph.entity;

import java.util.Objects;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description InternalQlPair
 */
public class InternalQlPair<K, V> implements QlPair<K, V> {

    private final K key;
    private final V value;

    protected InternalQlPair(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        this.key = key;
        this.value = value;
    }

    public K key() {
        return this.key;
    }

    public V value() {
        return this.value;
    }

    public static <K, V> QlPair<K, V> of(K key, V value) {
        return new InternalQlPair(key, value);
    }

    public String toString() {
        return String.format("%s: %s", Objects.toString(this.key), Objects.toString(this.value));
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            InternalQlPair<?, ?> that = (InternalQlPair)o;
            return this.key.equals(that.key) && this.value.equals(that.value);
        } else {
            return false;
        }
    }

    public int hashCode() {
        int result = this.key.hashCode();
        result = 31 * result + this.value.hashCode();
        return result;
    }

}
