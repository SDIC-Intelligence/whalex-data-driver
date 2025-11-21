package com.meiya.whalex.interior.db.search.condition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

/**
 * 聚合方法类型
 *
 * @author 黄河森
 * @date 2020/12/15
 * @project whalex-data-driver
 */
public enum AggFunctionType {

    SUM("sum"),
    AVG("avg"),
    MAX("max"),
    MIN("min"),
    COUNT("count"),
    DISTINCT("distinct"),
    PERCENTILES("percentiles"),
    DATE_HISTOGRAM("date_histogram");

    private String type;

    AggFunctionType(String type) {
        this.type = type;
    }

    @JsonValue
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @JsonCreator
    public static AggFunctionType findType(String type) {
        for (AggFunctionType value : AggFunctionType.values()) {
            if (StringUtils.equalsIgnoreCase(value.getType(), type)) {
                return value;
            }
        }
        throw new RuntimeException("No enum constant com.meiya.whalex.interior.db.search.condition.AggFunctionType." + type);
    }

    @Override
    public String toString() {
        return getType();
    }
}
