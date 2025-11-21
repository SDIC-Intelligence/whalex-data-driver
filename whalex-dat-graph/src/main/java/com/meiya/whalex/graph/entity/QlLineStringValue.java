package com.meiya.whalex.graph.entity;

import java.util.List;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlPointValue
 */
public class QlLineStringValue extends QlObjectValueAdapter<List<? extends QlPoint>> {

    public QlLineStringValue(List<? extends QlPoint> lineString) {
        super(lineString);
    }

    public List<? extends QlPoint> asLineString() {
        return (List<? extends QlPoint>)this.asObject();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.POINT();
    }

}
