package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphNode;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphPathWrapper;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphRelationship;
import com.vesoft.nebula.client.graph.data.PathWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphPathWrapperImpl
 */
public class NebulaGraphPathWrapperImpl implements NebulaGraphPathWrapper {

    private final PathWrapper pathWrapper;

    public NebulaGraphPathWrapperImpl(PathWrapper pathWrapper) {
        this.pathWrapper = pathWrapper;
    }

    @Override
    public String getDecodeType() {
        return pathWrapper.getDecodeType();
    }

    @Override
    public int getTimezoneOffset() {
        return pathWrapper.getTimezoneOffset();
    }

    @Override
    public NebulaGraphNode getStartNode() {
        return new NebulaGraphNodeImpl(pathWrapper.getStartNode());
    }

    @Override
    public NebulaGraphNode getEndNode() {
        return new NebulaGraphNodeImpl(pathWrapper.getEndNode());
    }

    @Override
    public boolean containNode(NebulaGraphNode node) {
        return pathWrapper.containNode(((NebulaGraphNodeImpl)node).getNode());
    }

    @Override
    public boolean containRelationship(NebulaGraphRelationship relationship) {
        return pathWrapper.containRelationship(((NebulaGraphRelationshipImpl) relationship).getRelationship());
    }

    @Override
    public List<NebulaGraphNode> getNodes() {
        return pathWrapper.getNodes().stream().flatMap(node -> Stream.of(new NebulaGraphNodeImpl(node)))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public List<NebulaGraphRelationship> getRelationships() {
        return pathWrapper.getRelationships().stream().flatMap(relationship -> Stream.of(new NebulaGraphRelationshipImpl(relationship)))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public List<NebulaGraphSegment> getSegments() {
        return pathWrapper.getSegments().stream().flatMap(segment -> Stream.of(new NebulaGraphSegmentImpl(segment)))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public int length() {
        return pathWrapper.length();
    }

    public static class NebulaGraphSegmentImpl implements NebulaGraphSegment {

        private final PathWrapper.Segment segment;

        public NebulaGraphSegmentImpl(PathWrapper.Segment segment) {
            this.segment = segment;
        }

        @Override
        public NebulaGraphNode getStartNode() {
            return new NebulaGraphNodeImpl(segment.getStartNode());
        }

        @Override
        public NebulaGraphRelationship getRelationShip() {
            return new NebulaGraphRelationshipImpl(segment.getRelationShip());
        }

        @Override
        public NebulaGraphNode getEndNode() {
            return new NebulaGraphNodeImpl(segment.getEndNode());
        }
    }
}
