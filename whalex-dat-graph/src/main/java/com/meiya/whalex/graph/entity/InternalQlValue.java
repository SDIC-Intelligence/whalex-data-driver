package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.enums.QlTypeConstructor;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description InternalQlValue
 */
public interface InternalQlValue extends QlValue, QlAsValue {

    QlTypeConstructor typeConstructor();

}
