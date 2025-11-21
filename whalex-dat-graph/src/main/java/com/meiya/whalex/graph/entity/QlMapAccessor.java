package com.meiya.whalex.graph.entity;

import java.util.Map;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlMapAccessor
 */
public interface QlMapAccessor {

    Iterable<String> keys();

    boolean containsKey(String var1);

    QlValue get(String var1);

    int size();

    Iterable<QlValue> values();

    <T> Iterable<T> values(Function<QlValue, T> var1);

    Map<String, Object> asMap();

    <T> Map<String, T> asMap(Function<QlValue, T> var1);

}
