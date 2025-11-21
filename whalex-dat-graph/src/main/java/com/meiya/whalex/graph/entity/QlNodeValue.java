package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlNodeValue
 */
public class QlNodeValue extends QlEntityValueAdapter<QlNode> {

    public QlNodeValue(QlNode adapted) {
        super(adapted);
    }

    public QlNode asNode() {
        return (QlNode)this.asEntity();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.NODE();
    }

}
