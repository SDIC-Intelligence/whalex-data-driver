package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.exception.QlClientException;
import com.meiya.whalex.graph.util.iterator.QlIterables;

import java.util.*;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlExtract
 */
public final class QlExtract {

    private QlExtract() {
        throw new UnsupportedOperationException();
    }

    public static List<QlValue> list(QlValue[] values) {
        switch (values.length) {
            case 0:
                return Collections.emptyList();
            case 1:
                return Collections.singletonList(values[0]);
            default:
                return Collections.unmodifiableList(Arrays.asList(values));
        }
    }

    public static <T> List<T> list(QlValue[] data, Function<QlValue, T> mapFunction) {
        int size = data.length;
        switch (size) {
            case 0:
                return Collections.emptyList();
            case 1:
                return Collections.singletonList(mapFunction.apply(data[0]));
            default:
                List<T> result = new ArrayList(size);
                QlValue[] var4 = data;
                int var5 = data.length;

                for(int var6 = 0; var6 < var5; ++var6) {
                    QlValue value = var4[var6];
                    result.add(mapFunction.apply(value));
                }

                return Collections.unmodifiableList(result);
        }
    }

    public static <T> Map<String, T> map(Map<String, QlValue> data, Function<QlValue, T> mapFunction) {
        if (data.isEmpty()) {
            return Collections.emptyMap();
        } else {
            int size = data.size();
            if (size == 1) {
                Map.Entry<String, QlValue> head = (Map.Entry)data.entrySet().iterator().next();
                return Collections.singletonMap(head.getKey(), mapFunction.apply(head.getValue()));
            } else {
                Map<String, T> map = QlIterables.newLinkedHashMapWithSize(size);
                Iterator var4 = data.entrySet().iterator();

                while(var4.hasNext()) {
                    Map.Entry<String, QlValue> entry = (Map.Entry)var4.next();
                    map.put(entry.getKey(), mapFunction.apply(entry.getValue()));
                }

                return Collections.unmodifiableMap(map);
            }
        }
    }

    public static <T> Map<String, T> map(QlRecord record, Function<QlValue, T> mapFunction) {
        int size = record.size();
        switch (size) {
            case 0:
                return Collections.emptyMap();
            case 1:
                return Collections.singletonMap(record.keys().get(0), mapFunction.apply(record.get(0)));
            default:
                Map<String, T> map = QlIterables.newLinkedHashMapWithSize(size);
                List<String> keys = record.keys();

                for(int i = 0; i < size; ++i) {
                    map.put(keys.get(i), mapFunction.apply(record.get(i)));
                }

                return Collections.unmodifiableMap(map);
        }
    }

    public static <V> Iterable<QlPair<String, V>> properties(QlMapAccessor map, Function<QlValue, V> mapFunction) {
        int size = map.size();
        switch (size) {
            case 0:
                return Collections.emptyList();
            case 1:
                String key = (String)map.keys().iterator().next();
                QlValue value = map.get(key);
                return Collections.singletonList(InternalQlPair.of(key, mapFunction.apply(value)));
            default:
                List<QlPair<String, V>> list = new ArrayList(size);
                Iterator var8 = map.keys().iterator();

                while(var8.hasNext()) {
                    String _key = (String)var8.next();
                    QlValue _value = map.get(_key);
                    list.add(InternalQlPair.of(_key, mapFunction.apply(_value)));
                }

                return Collections.unmodifiableList(list);
        }
    }

    public static <V> List<QlPair<String, V>> fields(QlRecord map, Function<QlValue, V> mapFunction) {
        int size = map.keys().size();
        switch (size) {
            case 0:
                return Collections.emptyList();
            case 1:
                String key = (String)map.keys().iterator().next();
                QlValue value = map.get(key);
                return Collections.singletonList(InternalQlPair.of(key, mapFunction.apply(value)));
            default:
                List<QlPair<String, V>> list = new ArrayList(size);
                List<String> keys = map.keys();

                for(int i = 0; i < size; ++i) {
                    String _key = (String)keys.get(i);
                    QlValue _value = map.get(i);
                    list.add(InternalQlPair.of(_key, mapFunction.apply(_value)));
                }

                return Collections.unmodifiableList(list);
        }
    }

    public static Map<String, QlValue> mapOfValues(Map<String, Object> map) {
        if (map != null && !map.isEmpty()) {
            Map<String, QlValue> result = QlIterables.newHashMapWithSize(map.size());
            Iterator var2 = map.entrySet().iterator();

            while(var2.hasNext()) {
                Map.Entry<String, Object> entry = (Map.Entry)var2.next();
                Object value = entry.getValue();
                assertParameter(value);
                result.put(entry.getKey(), QlValues.value(value));
            }

            return result;
        } else {
            return Collections.emptyMap();
        }
    }

    public static void assertParameter(Object value) {
        if (!(value instanceof QlNode) && !(value instanceof QlNodeValue)) {
            if (!(value instanceof QlRelationship) && !(value instanceof QlRelationshipValue)) {
                if (value instanceof QlPath || value instanceof QlPathValue) {
                    throw new QlClientException("Paths can't be used as parameters.");
                }
            } else {
                throw new QlClientException("Relationships can't be used as parameters.");
            }
        } else {
            throw new QlClientException("Nodes can't be used as parameters.");
        }
    }
    
}
