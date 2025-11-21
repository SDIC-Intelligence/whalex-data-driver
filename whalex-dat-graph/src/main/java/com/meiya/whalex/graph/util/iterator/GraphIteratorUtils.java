package com.meiya.whalex.graph.util.iterator;


import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 图数据库通用迭代器工具类
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.util
 * @project whalex-data-driver
 */
public class GraphIteratorUtils {

    private GraphIteratorUtils() {
    }

    public static final <S> Iterator<S> of(final S a) {
        return new GraphSingleIterator(a);
    }

    public static final <S> Iterator<S> of(final S a, S b) {
        return new GraphDoubleIterator(a, b);
    }

    public static final <S extends Collection<T>, T> S fill(final Iterator<T> iterator, final S collection) {
        while(iterator.hasNext()) {
            collection.add(iterator.next());
        }

        return collection;
    }

    public static void iterate(final Iterator iterator) {
        while(iterator.hasNext()) {
            iterator.next();
        }

    }

    public static final long count(final Iterator iterator) {
        long ix;
        for(ix = 0L; iterator.hasNext(); ++ix) {
            iterator.next();
        }

        return ix;
    }

    public static final long count(final Iterable iterable) {
        return count(iterable.iterator());
    }

    public static <S> List<S> list(final Iterator<S> iterator) {
        return (List)fill(iterator, new ArrayList());
    }

    public static <S> List<S> list(final Iterator<S> iterator, final Comparator comparator) {
        List<S> l = list(iterator);
        Collections.sort(l, comparator);
        return l;
    }

    public static <S> Set<S> set(final Iterator<S> iterator) {
        return (Set)fill(iterator, new HashSet());
    }

    public static <S> Iterator<S> limit(final Iterator<S> iterator, final int limit) {
        return new Iterator<S>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                return iterator.hasNext() && this.count < limit;
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public S next() {
                if (this.count++ >= limit) {
                    throw new NoSuchElementException();
                } else {
                    return iterator.next();
                }
            }
        };
    }

    public static <T> boolean allMatch(final Iterator<T> iterator, final Predicate<T> predicate) {
        while(true) {
            if (iterator.hasNext()) {
                if (predicate.test(iterator.next())) {
                    continue;
                }

                return false;
            }

            return true;
        }
    }

    public static <T> boolean anyMatch(final Iterator<T> iterator, final Predicate<T> predicate) {
        while(true) {
            if (iterator.hasNext()) {
                if (!predicate.test(iterator.next())) {
                    continue;
                }

                return true;
            }

            return false;
        }
    }

    public static <T> boolean noneMatch(final Iterator<T> iterator, final Predicate<T> predicate) {
        while(true) {
            if (iterator.hasNext()) {
                if (!predicate.test(iterator.next())) {
                    continue;
                }

                return false;
            }

            return true;
        }
    }

    public static <K, S> Map<K, S> collectMap(final Iterator<S> iterator, final Function<S, K> key) {
        return collectMap(iterator, key, Function.identity());
    }

    public static <K, S, V> Map<K, V> collectMap(final Iterator<S> iterator, final Function<S, K> key, final Function<S, V> value) {
        HashMap map = new HashMap();

        while(iterator.hasNext()) {
            S obj = iterator.next();
            map.put(key.apply(obj), value.apply(obj));
        }

        return map;
    }

    public static <K, S> Map<K, List<S>> groupBy(final Iterator<S> iterator, final Function<S, K> groupBy) {
        HashMap map = new HashMap();

        while(iterator.hasNext()) {
            S obj = iterator.next();
            ((List)map.computeIfAbsent(groupBy.apply(obj), (k) -> {
                return new ArrayList();
            })).add(obj);
        }

        return map;
    }

    public static <S> S reduce(final Iterator<S> iterator, final S identity, final BinaryOperator<S> accumulator) {
        Object result;
        for(result = identity; iterator.hasNext(); result = accumulator.apply((S) result, iterator.next())) {
        }

        return (S) result;
    }

    public static <S> S reduce(final Iterable<S> iterable, final S identity, final BinaryOperator<S> accumulator) {
        return reduce(iterable.iterator(), identity, accumulator);
    }

    public static <S, E> E reduce(final Iterator<S> iterator, final E identity, final BiFunction<E, S, E> accumulator) {
        Object result;
        for(result = identity; iterator.hasNext(); result = accumulator.apply((E) result, iterator.next())) {
        }

        return (E) result;
    }

    public static <S, E> E reduce(final Iterable<S> iterable, final E identity, final BiFunction<E, S, E> accumulator) {
        return reduce(iterable.iterator(), identity, accumulator);
    }

    public static final <S> Iterator<S> consume(final Iterator<S> iterator, final Consumer<S> consumer) {
        return new Iterator<S>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public S next() {
                S s = iterator.next();
                consumer.accept(s);
                return s;
            }
        };
    }

    public static final <S> Iterable<S> consume(final Iterable<S> iterable, final Consumer<S> consumer) {
        return () -> {
            return consume(iterable.iterator(), consumer);
        };
    }

    public static final <S, E> Iterator<E> map(final Iterator<S> iterator, final Function<S, E> function) {
        return new Iterator<E>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public E next() {
                return function.apply(iterator.next());
            }
        };
    }

    public static final <S, E> Iterable<E> map(final Iterable<S> iterable, final Function<S, E> function) {
        return () -> {
            return map(iterable.iterator(), function);
        };
    }

    public static final <S> Iterator<S> filter(final Iterator<S> iterator, final Predicate<S> predicate) {
        return new Iterator<S>() {
            S nextResult = null;

            @Override
            public boolean hasNext() {
                if (null != this.nextResult) {
                    return true;
                } else {
                    this.advance();
                    return null != this.nextResult;
                }
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public S next() {
                Object var1;
                try {
                    if (null != this.nextResult) {
                        var1 = this.nextResult;
                        return (S) var1;
                    }

                    this.advance();
                    if (null == this.nextResult) {
                        throw new NoSuchElementException();
                    }

                    var1 = this.nextResult;
                } finally {
                    this.nextResult = null;
                }

                return (S) var1;
            }

            private final void advance() {
                this.nextResult = null;

                Object s;
                do {
                    if (!iterator.hasNext()) {
                        return;
                    }

                    s = iterator.next();
                } while(!predicate.test((S) s));

                this.nextResult = (S) s;
            }
        };
    }

    public static final <S> Iterable<S> filter(final Iterable<S> iterable, final Predicate<S> predicate) {
        return () -> {
            return filter(iterable.iterator(), predicate);
        };
    }

    public static final <S, E> Iterator<E> flatMap(final Iterator<S> iterator, final Function<S, Iterator<E>> function) {
        return new Iterator<E>() {
            private Iterator<E> currentIterator = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                if (this.currentIterator.hasNext()) {
                    return true;
                } else {
                    do {
                        if (!iterator.hasNext()) {
                            return false;
                        }

                        this.currentIterator = (Iterator)function.apply(iterator.next());
                    } while(!this.currentIterator.hasNext());

                    return true;
                }
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public E next() {
                if (this.hasNext()) {
                    return this.currentIterator.next();
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    public static final <S> Iterator<S> concat(final Iterator<S>... iterators) {
        GraphMultiIterator<S> iterator = new GraphMultiIterator();
        Iterator[] var2 = iterators;
        int var3 = iterators.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            Iterator<S> itty = var2[var4];
            iterator.addIterator(itty);
        }

        return iterator;
    }

    public static Iterator asIterator(final Object o) {
        Object itty;
        if (o instanceof Iterable) {
            itty = ((Iterable)o).iterator();
        } else if (o instanceof Iterator) {
            itty = (Iterator)o;
        } else if (o instanceof Object[]) {
            itty = new GraphArrayIterator((Object[])((Object[])o));
        } else if (o instanceof Stream) {
            itty = ((Stream)o).iterator();
        } else if (o instanceof Map) {
            itty = ((Map)o).entrySet().iterator();
        } else if (o instanceof Throwable) {
            itty = of(((Throwable)o).getMessage());
        } else {
            itty = of(o);
        }

        return (Iterator)itty;
    }

    public static List asList(final Object o) {
        return list(asIterator(o));
    }

    public static <T> Stream<T> stream(final Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 1088), false);
    }

    public static <T> Stream<T> stream(final Iterable<T> iterable) {
        return stream(iterable.iterator());
    }

    public static <T> Iterator<T> noRemove(final Iterator<T> iterator) {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
            }

            @Override
            public T next() {
                return iterator.next();
            }
        };
    }

    public static <T> Iterator<T> removeOnNext(final Iterator<T> iterator) {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public T next() {
                T object = iterator.next();
                iterator.remove();
                return object;
            }
        };
    }
}
