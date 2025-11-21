package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlType
 */
public interface QlType {

    String name();

    boolean isTypeOf(QlValue var1);

}
