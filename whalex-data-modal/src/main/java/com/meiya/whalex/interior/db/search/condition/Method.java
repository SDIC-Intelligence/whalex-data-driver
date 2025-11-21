package com.meiya.whalex.interior.db.search.condition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 函数/聚合方法
 *
 * @author 黄河森
 * @date 2019/9/10
 * @project whale-cloud-platformX
 */
public enum Method {

    SUM("sum"),
    COUNT("count"),
    MAX("max"),
    MIN("min"),
    AVG("avg"),
    AS_TEXT("AsText"),
    NULL(""),
    ;

    private final String name;

    Method(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    @JsonCreator
    public static Method parse(String name) {
        switch (name.toLowerCase()) {
            case "sum":
                return Method.SUM;
            case "count":
                return Method.COUNT;
            case "max":
                return Method.MAX;
            case "min":
                return Method.MIN;
            case "astext":
                return Method.AS_TEXT;
            default:
                return Method.NULL;
        }
    }

    @Override
    public String toString() {
        return getName();
    }
}
