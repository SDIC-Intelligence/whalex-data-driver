package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
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
@ApiModel(value = "库查询请求报文")
@Data
public class ListDatabaseEntity {

    /**
     * 组件信息
     */
    @ApiModelProperty(value = "云组件信息")
    private DbHandleEntity dbHandleEntity;

    @ApiModelProperty(value = "过滤条件")
    private QueryDatabasesCondition condition;

}
