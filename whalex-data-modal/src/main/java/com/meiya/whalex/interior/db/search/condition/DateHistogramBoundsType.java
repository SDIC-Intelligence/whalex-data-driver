package com.meiya.whalex.interior.db.search.condition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author 黄河森
 * @date 2024/1/4
 * @package com.meiya.whalex.interior.db.search.condition
 * @project whalex-data-driver
 * @description DateHistorgramBoundsType
 */
public enum DateHistogramBoundsType {

    EXTENDED_BOUNDS("extended_bounds"),
    HARD_BOUNDS("hard_bounds"),
    ;

    private String op;

    DateHistogramBoundsType(String op) {
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
    public static DateHistogramBoundsType parse(String opType) {
        if (opType == null) {
            return DateHistogramBoundsType.EXTENDED_BOUNDS;
        }
        for (DateHistogramBoundsType boundsType : DateHistogramBoundsType.values()) {
            if (boundsType.op.equalsIgnoreCase(opType)) {
                return boundsType;
            }
        }
        return DateHistogramBoundsType.EXTENDED_BOUNDS;
    }

    @Override
    public String toString() {
        return getOp();
    }
}
