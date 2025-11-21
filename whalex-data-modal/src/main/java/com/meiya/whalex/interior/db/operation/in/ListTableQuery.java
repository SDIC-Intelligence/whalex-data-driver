package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * @author 黄河森
 * @date 2022/3/7
 * @package com.meiya.whalex.interior.db.operation.in
 * @project whalex-data-driver
 */
@ApiModel(value = "库表查询请求报文")
@Data
public class ListTableQuery extends DatabaseQuery {

    @ApiModelProperty(value = "过滤条件")
    private QueryTablesCondition condition;

}
