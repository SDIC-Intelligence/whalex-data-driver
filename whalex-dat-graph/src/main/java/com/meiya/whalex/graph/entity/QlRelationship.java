package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlRelationship
 */
public interface QlRelationship extends QlEntity {

    long startNodeId();

    long endNodeId();

    String type();

    boolean hasType(String var1);

}
