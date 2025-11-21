package com.meiya.whalex.graph.util.iterator;

import java.util.*;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.util.iterator
 * @project whalex-data-driver
 * @description QlIterables
 */
public class QlIterables {

    private static final Queue EMPTY_QUEUE = new EmptyQueue();
    private static final float DEFAULT_HASH_MAP_LOAD_FACTOR = 0.75F;

    public QlIterables() {
    }

    public static int count(Iterable<?> it) {
        if (it instanceof Collection) {
            return ((Collection)it).size();
        } else {
            int size = 0;

            for(Iterator var2 = it.iterator(); var2.hasNext(); ++size) {
                Object o = var2.next();
            }

            return size;
        }
    }

    public static <T> List<T> asList(Iterable<T> it) {
        if (it instanceof List) {
            return (List)it;
        } else {
            List<T> list = new ArrayList();
            Iterator var2 = it.iterator();

            while(var2.hasNext()) {
                T t = (T) var2.next();
                list.add(t);
            }

            return list;
        }
    }

    public static <T> T single(Iterable<T> it) {
        Iterator<T> iterator = it.iterator();
        if (!iterator.hasNext()) {
            throw new IllegalArgumentException("Given iterable is empty");
        } else {
            T result = iterator.next();
            if (iterator.hasNext()) {
                throw new IllegalArgumentException("Given iterable contains more than one element: " + it);
            } else {
                return result;
            }
        }
    }

    public static Map<String, String> map(String... alternatingKeyValue) {
        Map<String, String> out = newHashMapWithSize(alternatingKeyValue.length / 2);

        for(int i = 0; i < alternatingKeyValue.length; i += 2) {
            out.put(alternatingKeyValue[i], alternatingKeyValue[i + 1]);
        }

        return out;
    }

    public static <A, B> Iterable<B> map(final Iterable<A> it, final Function<A, B> f) {
        return new Iterable<B>() {
            public Iterator<B> iterator() {
                final Iterator<A> aIterator = it.iterator();
                return new Iterator<B>() {
                    public boolean hasNext() {
                        return aIterator.hasNext();
                    }

                    public B next() {
                        return f.apply(aIterator.next());
                    }

                    public void remove() {
                        aIterator.remove();
                    }
                };
            }
        };
    }

    public static <T> Queue<T> emptyQueue() {
        return EMPTY_QUEUE;
    }

    public static <K, V> HashMap<K, V> newHashMapWithSize(int expectedSize) {
        return new HashMap(hashMapCapacity(expectedSize));
    }

    public static <K, V> LinkedHashMap<K, V> newLinkedHashMapWithSize(int expectedSize) {
        return new LinkedHashMap(hashMapCapacity(expectedSize));
    }

    private static int hashMapCapacity(int expectedSize) {
        if (expectedSize < 3) {
            if (expectedSize < 0) {
                throw new IllegalArgumentException("Illegal map size: " + expectedSize);
            } else {
                return expectedSize + 1;
            }
        } else {
            return (int)((float)expectedSize / 0.75F + 1.0F);
        }
    }

    private static class EmptyQueue<T> extends AbstractQueue<T> {
        private EmptyQueue() {
        }

        public Iterator<T> iterator() {
            return Collections.emptyIterator();
        }

        public int size() {
            return 0;
        }

        public boolean offer(T t) {
            throw new UnsupportedOperationException();
        }

        public T poll() {
            return null;
        }

        public T peek() {
            return null;
        }
    }

}
