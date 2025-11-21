package com.meiya.whalex.graph.entity;

import java.time.LocalDateTime;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlLocalDateTimeValue
 */
public class QlLocalDateTimeValue extends QlObjectValueAdapter<LocalDateTime> {

    public QlLocalDateTimeValue(LocalDateTime localDateTime) {
        super(localDateTime);
    }

    public LocalDateTime asLocalDateTime() {
        return (LocalDateTime)this.asObject();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME();
    }

}
