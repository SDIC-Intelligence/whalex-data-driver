package com.meiya.whalex.interior.db.search.condition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;

/**
 * 聚合操作类型
 *
 * @author 黄河森
 * @date 2021/1/13
 * @project whalex-data-driver
 */
public enum  AggOpType implements Serializable {

    TERMS("terms"),
    GROUP("group"),
    /*
    直方图聚合
     */
    HISTOGRAM("histogram"),
    /*
    日期直方图聚合
     */
    DATE_HISTOGRAM("date_histogram"),
    /**
     * 内部嵌套对象聚合
     */
    NESTED("nested"),
    /**
     * 排重
     */
    DISTINCT("distinct"),
    /**
     * 自定义范围查询
     */
    RANGE("range"),
    ;

    private String op;

    AggOpType(String op) {
        this.op = op;
    }

    @JsonValue
    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    @JsonCreator
    public static AggOpType parse(String opType) {
        if (opType == null) {
            return AggOpType.TERMS;
        }
        for (AggOpType aggOpType : AggOpType.values()) {
            if (aggOpType.op.equalsIgnoreCase(opType)) {
                return aggOpType;
            }
        }
        return AggOpType.TERMS;
    }

    @Override
    public String toString() {
        return getOp();
    }
}
