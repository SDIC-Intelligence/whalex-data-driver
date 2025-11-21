package com.meiya.whalex.graph.entity;

import java.time.temporal.TemporalAmount;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlIsoDuration
 */
public interface QlIsoDuration extends TemporalAmount {

    long months();

    long days();

    long seconds();

    int nanoseconds();

}
