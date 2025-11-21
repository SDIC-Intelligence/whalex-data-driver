package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphGeographyWrapper;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphLineStringWrapper;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphPointWrapper;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphPolygonWrapper;
import com.vesoft.nebula.Geography;
import com.vesoft.nebula.client.graph.data.GeographyWrapper;
import com.vesoft.nebula.client.graph.data.LineStringWrapper;
import com.vesoft.nebula.client.graph.data.PointWrapper;
import com.vesoft.nebula.client.graph.data.PolygonWrapper;

import java.lang.reflect.Field;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphGeographyWrapperImpl
 */
public class NebulaGraphGeographyWrapperImpl implements NebulaGraphGeographyWrapper {

    private final GeographyWrapper geographyWrapper;

    public NebulaGraphGeographyWrapperImpl(GeographyWrapper geographyWrapper) {
        this.geographyWrapper = geographyWrapper;
    }

    @Override
    public String getDecodeType() {
        return geographyWrapper.getDecodeType();
    }

    @Override
    public int getTimezoneOffset() {
        return geographyWrapper.getTimezoneOffset();
    }

    @Override
    public NebulaGraphPolygonWrapper getPolygonWrapper() {
        PolygonWrapper polygonWrapper = geographyWrapper.getPolygonWrapper();
        return new NebulaGraphPolygonWrapperImpl(polygonWrapper);
    }

    @Override
    public NebulaGraphLineStringWrapper getLineStringWrapper() {
        LineStringWrapper lineStringWrapper = geographyWrapper.getLineStringWrapper();
        return new NebulaGraphLineStringWrapperImpl(lineStringWrapper);
    }

    @Override
    public NebulaGraphPointWrapper getPointWrapper() {
        PointWrapper pointWrapper = geographyWrapper.getPointWrapper();
        return new NebulaGraphPointWrapperImpl(pointWrapper);
    }

    @Override
    public boolean isPolygon() {
        try {
            Field declaredField = GeographyWrapper.class.getDeclaredField("geography");
            Geography geography = (Geography) declaredField.get(geographyWrapper);
            int setField = geography.getSetField();
            if (setField == 3) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isLineString() {
        try {
            Field declaredField = GeographyWrapper.class.getDeclaredField("geography");
            Geography geography = (Geography) declaredField.get(geographyWrapper);
            int setField = geography.getSetField();
            if (setField == 2) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isPoint() {
        try {
            Field declaredField = GeographyWrapper.class.getDeclaredField("geography");
            Geography geography = (Geography) declaredField.get(geographyWrapper);
            int setField = geography.getSetField();
            if (setField == 1) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
