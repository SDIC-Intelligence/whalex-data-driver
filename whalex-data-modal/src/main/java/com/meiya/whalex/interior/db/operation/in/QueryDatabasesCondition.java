package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * 查询库过滤条件
 *
 * @author 黄河森
 * @date 2022/3/4
 * @package com.meiya.whalex.db.entity
 * @project whalex-data-driver
 */
@ApiModel(value = "库查询过滤条件")
@Data
public class QueryDatabasesCondition {

    @ApiModelProperty(value = "库名匹配规则", notes = "可携带 ? * 占位符")
    private String databaseMatch;

}
