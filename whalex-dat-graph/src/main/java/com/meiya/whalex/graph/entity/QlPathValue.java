package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlPathValue
 */
public class QlPathValue extends QlObjectValueAdapter<QlPath> {

    public QlPathValue(QlPath adapted) {
        super(adapted);
    }

    public QlPath asPath() {
        return (QlPath)this.asObject();
    }

    public int size() {
        return ((QlPath)this.asObject()).length();
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.PATH();
    }

}
