package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphCoordinateWrapper;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphLineStringWrapper;
import com.vesoft.nebula.client.graph.data.CoordinateWrapper;
import com.vesoft.nebula.client.graph.data.LineStringWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphLineStringWrapperImpl
 */
public class NebulaGraphLineStringWrapperImpl implements NebulaGraphLineStringWrapper {

    private final LineStringWrapper lineStringWrapper;

    public NebulaGraphLineStringWrapperImpl(LineStringWrapper lineStringWrapper) {
        this.lineStringWrapper = lineStringWrapper;
    }

    @Override
    public String getDecodeType() {
        return lineStringWrapper.getDecodeType();
    }

    @Override
    public int getTimezoneOffset() {
        return lineStringWrapper.getTimezoneOffset();
    }

    @Override
    public List<NebulaGraphCoordinateWrapper> getCoordinateList() {
        List<CoordinateWrapper> coordinateList = lineStringWrapper.getCoordinateList();
        return coordinateList.stream().flatMap(coordinateWrapper -> Stream.of(new NebulaGraphCoordinateWrapperImpl(coordinateWrapper)))
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
