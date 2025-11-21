package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.GraphElement;
import com.meiya.whalex.graph.entity.GraphProperty;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author 黄河森
 * @date 2023/3/24
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class Neo4jProperty<V> implements GraphProperty<V> {

    private String key;

    private V value;

    public Neo4jProperty(String key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public GraphElement element() {
        return null;
    }
}
