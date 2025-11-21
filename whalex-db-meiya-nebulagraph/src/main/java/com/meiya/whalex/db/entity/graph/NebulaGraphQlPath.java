package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.*;
import com.meiya.whalex.graph.exception.QlUnsupportedEncodingException;
import com.vesoft.nebula.client.graph.data.Node;
import com.vesoft.nebula.client.graph.data.PathWrapper;
import com.vesoft.nebula.client.graph.data.Relationship;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 * @description NebulaGraphQlPath
 */
public class NebulaGraphQlPath extends InternalQlPath {

    public NebulaGraphQlPath(List<QlEntity> alternatingNodeAndRel) {
        super(alternatingNodeAndRel);
    }

    public NebulaGraphQlPath(QlEntity... alternatingNodeAndRel) {
        super(alternatingNodeAndRel);
    }

    public NebulaGraphQlPath(List<Segment> segments, List<QlNode> nodes, List<QlRelationship> relationships) {
        super(segments, nodes, relationships);
    }

    public static class Builder {
        public static NebulaGraphQlPath build(PathWrapper pathWrapper) throws UnsupportedEncodingException {
            List<PathWrapper.Segment> segments = pathWrapper.getSegments();
            List<Node> nodes = pathWrapper.getNodes();
            List<Relationship> relationships = pathWrapper.getRelationships();

            List<QlNode> nebulaGraphQlNodes = nodes.stream().flatMap(node -> {
                try {
                    return Stream.of(NebulaGraphQlNode.Builder.build(node));
                } catch (UnsupportedEncodingException e) {
                    throw new QlUnsupportedEncodingException(e.getMessage());
                }
            }).collect(Collectors.toList());


            List<QlRelationship> nebulaGraphQlRelationships = relationships.stream().flatMap(relationship -> {
                try {
                    return Stream.of(NebulaGraphQlRelationship.Builder.build(relationship));
                } catch (UnsupportedEncodingException e) {
                    throw new QlUnsupportedEncodingException(e.getMessage());
                }
            }).collect(Collectors.toList());

            List<Segment> selfContainedSegments = segments.stream().flatMap(segment -> {
                try {
                    return Stream.of(NebulaGraphSelfContainedSegment.Builder.build(segment));
                } catch (UnsupportedEncodingException e) {
                    throw new QlUnsupportedEncodingException(e.getMessage());
                }
            }).collect(Collectors.toList());

            return new NebulaGraphQlPath(selfContainedSegments, nebulaGraphQlNodes, nebulaGraphQlRelationships);

        }
    }

    public static class NebulaGraphSelfContainedSegment extends SelfContainedSegment {

        public NebulaGraphSelfContainedSegment(QlNode start, QlRelationship relationship, QlNode end) {
            super(start, relationship, end);
        }

        public static class Builder {
            public static NebulaGraphSelfContainedSegment build(PathWrapper.Segment segment) throws UnsupportedEncodingException {
                Node startNode = segment.getStartNode();
                Node endNode = segment.getEndNode();
                Relationship relationShip = segment.getRelationShip();
                return new NebulaGraphSelfContainedSegment(NebulaGraphQlNode.Builder.build(startNode), NebulaGraphQlRelationship.Builder.build(relationShip), NebulaGraphQlNode.Builder.build(endNode));
            }
        }
    }
}
