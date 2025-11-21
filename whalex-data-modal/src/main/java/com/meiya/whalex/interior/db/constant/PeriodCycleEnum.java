package com.meiya.whalex.interior.db.constant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 周期值枚举
 *
 * @author 黄河森
 * @date 2021/6/22
 * @project whalex-data-driver-back
 */
public enum PeriodCycleEnum {

    DAY("d"),
    MONTH("m"),
    YEAR("y"),
    ONLY_ONE("only_one")
    ;

    private String period;

    PeriodCycleEnum(String period) {
        this.period = period;
    }
    @JsonValue
    public String getPeriod() {
        return period;
    }

    @JsonCreator
    public static PeriodCycleEnum parse(String period) {
        if (period == null) {
            return PeriodCycleEnum.ONLY_ONE;
        }
        for (PeriodCycleEnum periodCycleEnum : PeriodCycleEnum.values()) {
            if (periodCycleEnum.period.equalsIgnoreCase(period)) {
                return periodCycleEnum;
            }
        }
        return PeriodCycleEnum.ONLY_ONE;
    }

    @Override
    public String toString() {
        return getPeriod();
    }
}
