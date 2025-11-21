package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphGeographyWrapper
 */
public interface NebulaGraphGeographyWrapper extends NebulaGraphBaseDataObject {

    NebulaGraphPolygonWrapper getPolygonWrapper();

    NebulaGraphLineStringWrapper getLineStringWrapper();

    NebulaGraphPointWrapper getPointWrapper();

    boolean isPolygon();

    boolean isLineString();

    boolean isPoint();
}
