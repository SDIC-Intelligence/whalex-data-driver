package com.meiya.whalex.sql2dsl.enums;

import com.meiya.whalex.interior.db.search.condition.AggOpType;
import lombok.Getter;

/**
 * 特殊分组函数
 *
 * @author 黄河森
 * @date 2024/1/2
 * @package com.meiya.whalex.sql2dsl.enums
 * @project whalex-data-driver
 * @description GroupByFunctionEnum
 */
@Getter
public enum GroupByFunctionEnum {

    DATE_FORMAT(AggOpType.DATE_HISTOGRAM),
    DATE_HISTOGRAM(AggOpType.DATE_HISTOGRAM)
    ;

    private final AggOpType opType;

    GroupByFunctionEnum(AggOpType opType) {
        this.opType = opType;
    }
}
