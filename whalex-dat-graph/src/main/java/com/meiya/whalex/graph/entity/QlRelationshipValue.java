package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlRelationshipValue
 */
public class QlRelationshipValue extends QlEntityValueAdapter<QlRelationship> {

    public QlRelationshipValue(QlRelationship adapted) {
        super(adapted);
    }

    public QlRelationship asRelationship() {
        return (QlRelationship)this.asEntity();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.RELATIONSHIP();
    }

}
