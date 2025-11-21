package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphCoordinateWrapper;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphPointWrapper;
import com.vesoft.nebula.client.graph.data.PointWrapper;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphPointWrapperImpl
 */
public class NebulaGraphPointWrapperImpl implements NebulaGraphPointWrapper {

    private final PointWrapper pointWrapper;

    public NebulaGraphPointWrapperImpl(PointWrapper pointWrapper) {
        this.pointWrapper = pointWrapper;
    }

    @Override
    public String getDecodeType() {
        return pointWrapper.getDecodeType();
    }

    @Override
    public int getTimezoneOffset() {
        return pointWrapper.getTimezoneOffset();
    }

    @Override
    public NebulaGraphCoordinateWrapper getCoordinate() {
        return new NebulaGraphCoordinateWrapperImpl(pointWrapper.getCoordinate());
    }
}
