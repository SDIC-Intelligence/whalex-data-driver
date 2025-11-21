package com.meiya.whalex.graph.entity;

import java.util.List;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlRecord
 */
public interface QlRecord extends QlMapAccessorWithDefaultValue {

    List<String> keys();

    List<QlValue> values();

    int index(String var1);

    QlValue get(int var1);

    List<QlPair<String, QlValue>> fields();

}
