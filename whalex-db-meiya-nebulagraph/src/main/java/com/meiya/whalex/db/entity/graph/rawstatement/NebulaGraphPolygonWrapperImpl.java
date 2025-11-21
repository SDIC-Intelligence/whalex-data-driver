package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphCoordinateWrapper;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphPolygonWrapper;
import com.vesoft.nebula.client.graph.data.CoordinateWrapper;
import com.vesoft.nebula.client.graph.data.PolygonWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphPolygonWrapperImpl
 */
public class NebulaGraphPolygonWrapperImpl implements NebulaGraphPolygonWrapper {

    private final PolygonWrapper polygonWrapper;

    public NebulaGraphPolygonWrapperImpl(PolygonWrapper polygonWrapper) {
        this.polygonWrapper = polygonWrapper;
    }

    @Override
    public List<List<NebulaGraphCoordinateWrapper>> getCoordListList() {
        List<List<CoordinateWrapper>> coordListList = polygonWrapper.getCoordListList();
        List<List<NebulaGraphCoordinateWrapper>> coordinateWrapper2List = new ArrayList<>(coordListList.size());
        for (List<CoordinateWrapper> coordinateWrappers : coordListList) {
            ArrayList<NebulaGraphCoordinateWrapper> collect = coordinateWrappers.stream().flatMap(coordinateWrapper -> Stream.of(new NebulaGraphCoordinateWrapperImpl(coordinateWrapper)))
                    .collect(Collectors.toCollection(ArrayList::new));
            coordinateWrapper2List.add(collect);
        }
        return coordinateWrapper2List;
    }
}
