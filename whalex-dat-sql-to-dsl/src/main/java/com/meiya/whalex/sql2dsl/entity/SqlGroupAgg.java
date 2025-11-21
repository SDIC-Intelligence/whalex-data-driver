package com.meiya.whalex.sql2dsl.entity;

import com.meiya.whalex.interior.db.search.condition.AggOpType;
import lombok.Builder;
import lombok.Data;

/**
 * group by
 *
 * @author 黄河森
 * @date 2024/1/2
 * @package com.meiya.whalex.sql2dsl.entity
 * @project whalex-data-driver
 * @description SqlGroupAgg
 */
@Data
public class SqlGroupAgg extends SqlAgg {


    @Builder(toBuilder = true)
    public SqlGroupAgg(String aggName, String fieldName) {
        super(aggName, fieldName, AggOpType.GROUP);
    }
}
