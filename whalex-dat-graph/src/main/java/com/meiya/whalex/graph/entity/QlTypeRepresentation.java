package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.enums.QlTypeConstructor;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlTypeRepresentation
 */
public class QlTypeRepresentation implements QlType {

    private final QlTypeConstructor tyCon;

    public QlTypeRepresentation(QlTypeConstructor tyCon) {
        this.tyCon = tyCon;
    }

    public boolean isTypeOf(QlValue value) {
        return this.tyCon.covers(value);
    }

    public String name() {
        return this.tyCon == QlTypeConstructor.LIST ? "LIST OF ANY?" : this.tyCon.toString();
    }

    public QlTypeConstructor constructor() {
        return this.tyCon;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlTypeRepresentation that = (QlTypeRepresentation)o;
            return this.tyCon == that.tyCon;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return this.tyCon.hashCode();
    }

}
