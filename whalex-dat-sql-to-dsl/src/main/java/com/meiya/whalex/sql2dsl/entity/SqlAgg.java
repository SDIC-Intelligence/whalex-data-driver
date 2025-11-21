package com.meiya.whalex.sql2dsl.entity;

import com.meiya.whalex.interior.db.search.condition.AggOpType;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 黄河森
 * @date 2024/1/2
 * @package com.meiya.whalex.sql2dsl.entity
 * @project whalex-data-driver
 * @description SqlAgg
 */
@Data
@NoArgsConstructor
public class SqlAgg {

    protected String aggName;

    protected String fieldName;

    protected AggOpType opType;

    public SqlAgg(String aggName, String fieldName, AggOpType opType) {
        if (StringUtils.isBlank(aggName)) {
            if (StringUtils.isBlank(aggName) && StringUtils.isNotBlank(fieldName)) {
                aggName = fieldName + "_" + opType.getOp();
            }
        }
        this.aggName = aggName;
        this.fieldName = fieldName;
        this.opType = opType;
    }
}
