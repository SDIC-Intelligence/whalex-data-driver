package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlNode
 */
public interface QlNode extends QlEntity {

    Iterable<String> labels();

    boolean hasLabel(String var1);

}
