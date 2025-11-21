package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * 查询库表过滤条件
 *
 * @author 黄河森
 * @date 2022/3/4
 * @package com.meiya.whalex.db.entity
 * @project whalex-data-driver
 */
@ApiModel(value = "库表查询请求报文")
@Data
public class ListTableEntity {

    /**
     * 组件信息
     */
    @ApiModelProperty(value = "云组件信息")
    private DbHandleEntity dbHandleEntity;

    @ApiModelProperty(value = "过滤条件")
    private QueryTablesCondition condition;

}
