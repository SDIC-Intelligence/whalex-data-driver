package com.meiya.whalex.graph.entity;

import java.util.List;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlPointValue
 */
public class QlPolygonValue extends QlObjectValueAdapter<List<List<? extends QlPoint>>> {

    public QlPolygonValue(List<List<? extends QlPoint>> polygon) {
        super(polygon);
    }

    public List<List<? extends QlPoint>> asPolygon() {
        return (List<List<? extends QlPoint>>)this.asObject();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.POINT();
    }

}
