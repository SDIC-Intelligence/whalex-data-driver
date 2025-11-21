package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.enums.QlTypeConstructor;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description InternalQlTypeSystem
 */
public class InternalQlTypeSystem implements QlTypeSystem {

    public static InternalQlTypeSystem TYPE_SYSTEM = new InternalQlTypeSystem();
    private final QlTypeRepresentation anyType;
    private final QlTypeRepresentation booleanType;
    private final QlTypeRepresentation bytesType;
    private final QlTypeRepresentation stringType;
    private final QlTypeRepresentation numberType;
    private final QlTypeRepresentation integerType;
    private final QlTypeRepresentation floatType;
    private final QlTypeRepresentation listType;
    private final QlTypeRepresentation mapType;
    private final QlTypeRepresentation nodeType;
    private final QlTypeRepresentation relationshipType;
    private final QlTypeRepresentation pathType;
    private final QlTypeRepresentation pointType;
    private final QlTypeRepresentation dateType;
    private final QlTypeRepresentation timeType;
    private final QlTypeRepresentation localTimeType;
    private final QlTypeRepresentation localDateTimeType;
    private final QlTypeRepresentation dateTimeType;
    private final QlTypeRepresentation durationType;
    private final QlTypeRepresentation nullType;

    private InternalQlTypeSystem() {
        this.anyType = this.constructType(QlTypeConstructor.ANY);
        this.booleanType = this.constructType(QlTypeConstructor.BOOLEAN);
        this.bytesType = this.constructType(QlTypeConstructor.BYTES);
        this.stringType = this.constructType(QlTypeConstructor.STRING);
        this.numberType = this.constructType(QlTypeConstructor.NUMBER);
        this.integerType = this.constructType(QlTypeConstructor.INTEGER);
        this.floatType = this.constructType(QlTypeConstructor.FLOAT);
        this.listType = this.constructType(QlTypeConstructor.LIST);
        this.mapType = this.constructType(QlTypeConstructor.MAP);
        this.nodeType = this.constructType(QlTypeConstructor.NODE);
        this.relationshipType = this.constructType(QlTypeConstructor.RELATIONSHIP);
        this.pathType = this.constructType(QlTypeConstructor.PATH);
        this.pointType = this.constructType(QlTypeConstructor.POINT);
        this.dateType = this.constructType(QlTypeConstructor.DATE);
        this.timeType = this.constructType(QlTypeConstructor.TIME);
        this.localTimeType = this.constructType(QlTypeConstructor.LOCAL_TIME);
        this.localDateTimeType = this.constructType(QlTypeConstructor.LOCAL_DATE_TIME);
        this.dateTimeType = this.constructType(QlTypeConstructor.DATE_TIME);
        this.durationType = this.constructType(QlTypeConstructor.DURATION);
        this.nullType = this.constructType(QlTypeConstructor.NULL);
    }

    public QlType ANY() {
        return this.anyType;
    }

    public QlType BOOLEAN() {
        return this.booleanType;
    }

    public QlType BYTES() {
        return this.bytesType;
    }

    public QlType STRING() {
        return this.stringType;
    }

    public QlType NUMBER() {
        return this.numberType;
    }

    public QlType INTEGER() {
        return this.integerType;
    }

    public QlType FLOAT() {
        return this.floatType;
    }

    public QlType LIST() {
        return this.listType;
    }

    public QlType MAP() {
        return this.mapType;
    }

    public QlType NODE() {
        return this.nodeType;
    }

    public QlType RELATIONSHIP() {
        return this.relationshipType;
    }

    public QlType PATH() {
        return this.pathType;
    }

    public QlType POINT() {
        return this.pointType;
    }

    public QlType DATE() {
        return this.dateType;
    }

    public QlType TIME() {
        return this.timeType;
    }

    public QlType LOCAL_TIME() {
        return this.localTimeType;
    }

    public QlType LOCAL_DATE_TIME() {
        return this.localDateTimeType;
    }

    public QlType DATE_TIME() {
        return this.dateTimeType;
    }

    public QlType DURATION() {
        return this.durationType;
    }

    public QlType NULL() {
        return this.nullType;
    }

    private QlTypeRepresentation constructType(QlTypeConstructor tyCon) {
        return new QlTypeRepresentation(tyCon);
    }
    
}
