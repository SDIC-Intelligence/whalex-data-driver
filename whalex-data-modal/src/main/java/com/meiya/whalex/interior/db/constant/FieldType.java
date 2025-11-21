package com.meiya.whalex.interior.db.constant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 返回结果参数类型枚举
 *
 * @author Huanghesen
 * @date 2018-9-10 11:05:00
 */
public enum FieldType {

    OBJECT("Object"),
    STRING("String"),
    BASE64_STRING("Base64String"),
    NUMBER("Number"),
    INT("Int"),
    LONG("Long"),
    BOOLEAN("Boolean"),
    DATE_TIME_STRING("DateTimeString"),
    JSON_OBJECT("JSONObject"),
    JSON_ARRAY("JSONArray"),
    BYTE("Byte"),
    CLOB("Clob"),
    DOUBLE("Double"),
    FLOAT("Float");


    private final String name;

    FieldType(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    @JsonCreator
    public static FieldType parse(String name) {
        switch (name) {
            case "String":
                return FieldType.STRING;
            case "Number":
                return FieldType.NUMBER;
            case "Int":
                return FieldType.INT;
            case "Long":
                return FieldType.LONG;
            case "Double":
                return FieldType.DOUBLE;
            case "Float":
                return FieldType.FLOAT;
            case "Boolean":
                return FieldType.BOOLEAN;
            case "DateTimeString":
                return FieldType.DATE_TIME_STRING;
            case "JSONObject":
                return FieldType.JSON_OBJECT;
            case "JSONArray":
                return FieldType.JSON_ARRAY;
            case "Byte":
                return FieldType.BYTE;
            case "Clob":
                return FieldType.CLOB;
            default:
                return FieldType.OBJECT;
        }
    }

}
