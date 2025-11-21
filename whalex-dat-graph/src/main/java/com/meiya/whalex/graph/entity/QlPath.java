package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlPath
 */
public interface QlPath extends Iterable<QlPath.Segment> {

    QlNode start();

    QlNode end();

    int length();

    boolean contains(QlNode var1);

    boolean contains(QlRelationship var1);

    Iterable<QlNode> nodes();

    Iterable<QlRelationship> relationships();

    public interface Segment {
        QlRelationship relationship();

        QlNode start();

        QlNode end();
    }


}
