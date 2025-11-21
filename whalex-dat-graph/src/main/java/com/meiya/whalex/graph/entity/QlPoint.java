package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlPoint
 */
public interface QlPoint {

    int srid();

    double x();

    double y();

    double z();

}
