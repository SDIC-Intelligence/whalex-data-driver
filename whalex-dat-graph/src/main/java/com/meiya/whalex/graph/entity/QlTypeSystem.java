package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlTypeSystem
 */
public interface QlTypeSystem {

    QlType ANY();

    QlType BOOLEAN();

    QlType BYTES();

    QlType STRING();

    QlType NUMBER();

    QlType INTEGER();

    QlType FLOAT();

    QlType LIST();

    QlType MAP();

    QlType NODE();

    QlType RELATIONSHIP();

    QlType PATH();

    QlType POINT();

    QlType DATE();

    QlType TIME();

    QlType LOCAL_TIME();

    QlType LOCAL_DATE_TIME();
    
}
