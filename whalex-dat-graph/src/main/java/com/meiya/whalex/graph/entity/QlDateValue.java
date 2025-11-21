package com.meiya.whalex.graph.entity;

import java.time.LocalDate;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlDateValue
 */
public class QlDateValue extends QlObjectValueAdapter<LocalDate> {

    public QlDateValue(LocalDate date) {
        super(date);
    }

    public LocalDate asLocalDate() {
        return (LocalDate)this.asObject();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.DATE();
    }

}
