package com.meiya.whalex.graph.entity;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 节点属性
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 */
public interface GraphProperty<V> {

    /**
     * 键
     * @return
     */
    String key();

    /**
     * 值
     *
     * @return
     * @throws NoSuchElementException
     */
    V value() throws NoSuchElementException;

    /**
     * 是否不为空
     *
     * @return
     */
    boolean isPresent();

    /**
     * 是否不为空
     *
     * @param consumer
     */
    default void ifPresent(final Consumer<? super V> consumer) {
        if (this.isPresent()) {
            consumer.accept(this.value());
        }

    }

    /**
     * 获取不到值则获取默认值
     *
     * @param otherValue
     * @return
     */
    default V orElse(final V otherValue) {
        return this.isPresent() ? this.value() : otherValue;
    }

    /**
     * 获取不到值则获取默认值
     *
     * @param valueSupplier
     * @return
     */
    default V orElseGet(final Supplier<? extends V> valueSupplier) {
        return this.isPresent() ? this.value() : valueSupplier.get();
    }

    /**
     * 获取不到值则抛出异常
     *
     * @param exceptionSupplier
     * @param <E>
     * @return
     * @throws E
     */
    default <E extends Throwable> V orElseThrow(final Supplier<? extends E> exceptionSupplier) throws E {
        if (this.isPresent()) {
            return this.value();
        } else {
            throw exceptionSupplier.get();
        }
    }

    /**
     * 获取节点
     * @return
     */
    GraphElement element();

    static <V> GraphProperty<V> empty() {
        return GraphEmptyProperty.instance();
    }
}
