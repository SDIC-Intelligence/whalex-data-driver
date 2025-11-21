package com.meiya.whalex.graph.entity;

import org.javatuples.Pair;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * 图数据库路径
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 */
public interface GraphPath extends Iterable<Object> {

    /**
     * 路径长度
     *
     * @return
     */
    default int size() {
        return this.objects().size();
    }

    /**
     * 路径是否为空
     *
     * @return
     */
    default boolean isEmpty() {
        return this.size() == 0;
    }

    /**
     * 路径末尾
     *
     * @param <A>
     * @return
     */
    default <A> A head() {
        return (A) this.objects().get(this.size() - 1);
    }

    /**
     * 根据标签获取路径上的对象
     *
     * @param label
     * @param <A>
     * @return
     * @throws IllegalArgumentException
     */
    default <A> A get(final String label) throws IllegalArgumentException {
        List<Object> objects = this.objects();
        List<Set<String>> labels = this.labels();
        Object object = null;

        for(int i = 0; i < labels.size(); ++i) {
            if (((Set)labels.get(i)).contains(label)) {
                if (null == object) {
                    object = objects.get(i);
                } else if (object instanceof List) {
                    ((List)object).add(objects.get(i));
                } else {
                    List list = new ArrayList(2);
                    list.add(object);
                    list.add(objects.get(i));
                    object = list;
                }
            }
        }

        if (null == object) {
            throw new IllegalArgumentException("The step with label " + label + " does not exist");
        } else {
            return (A) object;
        }
    }

    /**
     *
     * 获取路径上的指定对象
     *
     * @param pop
     * @param label
     * @param <A>
     * @return
     * @throws IllegalArgumentException
     */
    default <A> A get(final Pop pop, final String label) throws IllegalArgumentException {
        if (Pop.mixed == pop) {
            return this.get(label);
        } else {
            Object object;
            if (Pop.all == pop) {
                if (this.hasLabel(label)) {
                    object = this.get(label);
                    return object instanceof List ? (A) object : ((A)Collections.singletonList(object));
                } else {
                    return (A) Collections.emptyList();
                }
            } else {
                object = this.get(label);
                if (object instanceof List) {
                    return (A) (Pop.last == pop ? ((List)object).get(((List)object).size() - 1) : ((List)object).get(0));
                } else {
                    return (A) object;
                }
            }
        }
    }

    /**
     * 根据路径顺序获取对象
     *
     * @param index
     * @param <A>
     * @return
     */
    default <A> A get(final int index) {
        return (A) this.objects().get(index);
    }

    /**
     * 判断路径上是否包含指定标签
     *
     * @param label
     * @return
     */
    default boolean hasLabel(final String label) {
        return this.labels().stream().filter((labels) -> {
            return labels.contains(label);
        }).findAny().isPresent();
    }

    /**
     * 路径对象集合
     *
     * @return
     */
    List<Object> objects();

    /**
     * 路径标签集合
     *
     * @return
     */
    List<Set<String>> labels();

    /**
     * 路径闭环判断
     *
     * @return
     */
    default boolean isSimple() {
        List<Object> objects = this.objects();

        for(int i = 0; i < objects.size() - 1; ++i) {
            for(int j = i + 1; j < objects.size(); ++j) {
                if (objects.get(i).equals(objects.get(j))) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    default Iterator<Object> iterator() {
        return this.objects().iterator();
    }

    /**
     * 迭代遍历路径
     *
     * @param consumer
     */
    default void forEach(final BiConsumer<Object, Set<String>> consumer) {
        List<Object> objects = this.objects();
        List<Set<String>> labels = this.labels();

        for(int i = 0; i < objects.size(); ++i) {
            consumer.accept(objects.get(i), labels.get(i));
        }

    }

    /**
     * 流式对象
     *
     * @return
     */
    default Stream<Pair<Object, Set<String>>> stream() {
        List<Set<String>> labels = this.labels();
        List<Object> objects = this.objects();
        return IntStream.range(0, this.size()).mapToObj((i) -> {
            return Pair.with(objects.get(i), labels.get(i));
        });
    }

    default boolean popEquals(final Pop pop, final Object other) {
        if (!(other instanceof GraphPath)) {
            return false;
        } else {
            GraphPath otherPath = (GraphPath)other;
            return !this.labels().stream().flatMap(Collection::stream).filter((label) -> {
                return !otherPath.hasLabel(label) || !otherPath.get(pop, label).equals(this.get(pop, label));
            }).findAny().isPresent();
        }
    }

    enum Pop {
        first,
        last,
        all,
        mixed;

        private Pop() {
        }
    }
}
