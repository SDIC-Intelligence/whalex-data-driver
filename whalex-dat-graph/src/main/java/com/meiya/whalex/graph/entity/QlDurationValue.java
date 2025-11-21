package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlDurationValue
 */
public class QlDurationValue extends QlObjectValueAdapter<QlIsoDuration> {

    public QlDurationValue(QlIsoDuration duration) {
        super(duration);
    }

    public QlIsoDuration asIsoDuration() {
        return (QlIsoDuration)this.asObject();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.DURATION();
    }

}
