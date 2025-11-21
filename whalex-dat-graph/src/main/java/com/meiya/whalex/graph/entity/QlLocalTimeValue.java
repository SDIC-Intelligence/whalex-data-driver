package com.meiya.whalex.graph.entity;

import java.time.LocalTime;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlLocalTimeValue
 */
public class QlLocalTimeValue  extends QlObjectValueAdapter<LocalTime> {

    public QlLocalTimeValue(LocalTime time) {
        super(time);
    }

    public LocalTime asLocalTime() {
        return (LocalTime)this.asObject();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.LOCAL_TIME();
    }

}
