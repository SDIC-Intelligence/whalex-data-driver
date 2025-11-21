package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlPointValue
 */
public class QlPointValue extends QlObjectValueAdapter<QlPoint> {

    public QlPointValue(QlPoint point) {
        super(point);
    }

    public QlPoint asPoint() {
        return (QlPoint)this.asObject();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.POINT();
    }

}
