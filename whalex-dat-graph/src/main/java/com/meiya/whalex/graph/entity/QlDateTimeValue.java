package com.meiya.whalex.graph.entity;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlDateTimeValue
 */
public class QlDateTimeValue extends QlObjectValueAdapter<ZonedDateTime> {

    public QlDateTimeValue(ZonedDateTime zonedDateTime) {
        super(zonedDateTime);
    }

    public OffsetDateTime asOffsetDateTime() {
        return this.asZonedDateTime().toOffsetDateTime();
    }

    public ZonedDateTime asZonedDateTime() {
        return (ZonedDateTime)this.asObject();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.DATE_TIME();
    }

}
