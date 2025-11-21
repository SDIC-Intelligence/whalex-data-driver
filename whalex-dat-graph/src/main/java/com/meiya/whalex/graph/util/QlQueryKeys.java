package com.meiya.whalex.graph.util;

import java.util.*;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.util
 * @project whalex-data-driver
 * @description QlQueryKeys
 */
public class QlQueryKeys {

    private static final QlQueryKeys EMPTY = new QlQueryKeys(Collections.emptyList(), Collections.emptyMap());
    private final List<String> keys;
    private final Map<String, Integer> keyIndex;

    public QlQueryKeys(int size) {
        this(new ArrayList(size), new HashMap(size));
    }

    public QlQueryKeys(List<String> keys) {
        this.keys = keys;
        Map<String, Integer> keyIndex = new HashMap(keys.size());
        int i = 0;
        Iterator var4 = keys.iterator();

        while(var4.hasNext()) {
            String key = (String)var4.next();
            keyIndex.put(key, i++);
        }

        this.keyIndex = keyIndex;
    }

    public QlQueryKeys(List<String> keys, Map<String, Integer> keyIndex) {
        this.keys = keys;
        this.keyIndex = keyIndex;
    }

    public void add(String key) {
        int index = this.keys.size();
        this.keys.add(key);
        this.keyIndex.put(key, index);
    }

    public List<String> keys() {
        return this.keys;
    }

    public Map<String, Integer> keyIndex() {
        return this.keyIndex;
    }

    public static QlQueryKeys empty() {
        return EMPTY;
    }

    public int indexOf(String key) {
        return (Integer)this.keyIndex.getOrDefault(key, -1);
    }

    public boolean contains(String key) {
        return this.keyIndex.containsKey(key);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlQueryKeys queryKeys = (QlQueryKeys)o;
            return this.keys.equals(queryKeys.keys) && this.keyIndex.equals(queryKeys.keyIndex);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.keys, this.keyIndex});
    }

}
