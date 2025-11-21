package com.meiya.whalex.graph.entity;

import java.util.Map;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlEntityValueAdapter
 */
public abstract class QlEntityValueAdapter<V extends QlEntity> extends QlObjectValueAdapter<V> {

    protected QlEntityValueAdapter(V adapted) {
        super(adapted);
    }

    public V asEntity() {
        return this.asObject();
    }

    public Map<String, Object> asMap() {
        return this.asEntity().asMap();
    }

    public <T> Map<String, T> asMap(Function<QlValue, T> mapFunction) {
        return this.asEntity().asMap(mapFunction);
    }

    public int size() {
        return this.asEntity().size();
    }

    public Iterable<String> keys() {
        return this.asEntity().keys();
    }

    public QlValue get(String key) {
        return this.asEntity().get(key);
    }

}
