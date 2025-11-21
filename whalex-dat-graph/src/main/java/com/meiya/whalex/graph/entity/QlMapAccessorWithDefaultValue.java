package com.meiya.whalex.graph.entity;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlMapAccessorWithDefaultValue
 */
public interface QlMapAccessorWithDefaultValue extends QlMapAccessor {

    QlValue get(String var1, QlValue var2);

    Object get(String var1, Object var2);

    Number get(String var1, Number var2);

    QlEntity get(String var1, QlEntity var2);

    QlNode get(String var1, QlNode var2);

    QlPath get(String var1, QlPath var2);

    QlRelationship get(String var1, QlRelationship var2);

    List<Object> get(String var1, List<Object> var2);

    <T> List<T> get(String var1, List<T> var2, Function<QlValue, T> var3);

    Map<String, Object> get(String var1, Map<String, Object> var2);

    <T> Map<String, T> get(String var1, Map<String, T> var2, Function<QlValue, T> var3);

    int get(String var1, int var2);

    long get(String var1, long var2);

    boolean get(String var1, boolean var2);

    String get(String var1, String var2);

    float get(String var1, float var2);

    double get(String var1, double var2);

}
