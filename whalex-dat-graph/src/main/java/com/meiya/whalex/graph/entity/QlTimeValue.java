package com.meiya.whalex.graph.entity;

import java.time.OffsetTime;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlTimeValue
 */
public class QlTimeValue extends QlObjectValueAdapter<OffsetTime> {

    public QlTimeValue(OffsetTime time) {
        super(time);
    }

    public OffsetTime asOffsetTime() {
        return (OffsetTime)this.asObject();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.TIME();
    }

}
