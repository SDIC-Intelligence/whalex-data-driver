package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

import java.util.List;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphPathWrapper
 */
public interface NebulaGraphPathWrapper extends NebulaGraphBaseDataObject {

    NebulaGraphNode getStartNode();

    NebulaGraphNode getEndNode();

    boolean containNode(NebulaGraphNode node);

    boolean containRelationship(NebulaGraphRelationship relationship);

    List<NebulaGraphNode> getNodes();

    List<NebulaGraphRelationship> getRelationships();

    List<NebulaGraphSegment> getSegments();

    int length();

    interface NebulaGraphSegment {
        NebulaGraphNode getStartNode();

        NebulaGraphRelationship getRelationShip();

        NebulaGraphNode getEndNode();

    }

}
