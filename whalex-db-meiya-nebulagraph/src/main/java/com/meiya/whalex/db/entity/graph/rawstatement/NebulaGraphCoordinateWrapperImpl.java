package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphCoordinateWrapper;
import com.vesoft.nebula.client.graph.data.CoordinateWrapper;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphCoordinateWrapperImpl
 */
public class NebulaGraphCoordinateWrapperImpl implements NebulaGraphCoordinateWrapper {

    private final CoordinateWrapper coordinateWrapper;

    public NebulaGraphCoordinateWrapperImpl(CoordinateWrapper coordinateWrapper) {
        this.coordinateWrapper = coordinateWrapper;
    }

    @Override
    public String getDecodeType() {
        return coordinateWrapper.getDecodeType();
    }

    @Override
    public int getTimezoneOffset() {
        return coordinateWrapper.getTimezoneOffset();
    }

    @Override
    public double getX() {
        return coordinateWrapper.getX();
    }

    @Override
    public double getY() {
        return coordinateWrapper.getY();
    }
}
