package com.meiya.whalex.db.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * API 接口底层服务表删除统一入口
 *
 * @author 黄河森
 * @date 2019/9/11
 * @project whale-cloud-platformX
 */
@ApiModel(value = "组件清空表接口封装实体")
@Data
public class DbEmptyTableEntity {

    /**
     * 组件信息
     */
    @ApiModelProperty(value = "云组件信息")
    private DbHandleEntity dbHandleEntity;

    @ApiModelProperty(value = "清空表参数")
    private EmptyTableParamCondition emptyTableParamCondition;
}
