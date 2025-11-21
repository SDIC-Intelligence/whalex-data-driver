package com.meiya.whalex.db.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * API 接口底层服务表删除统一入口
 *
 * @author 黄河森
 * @date 2019/9/11
 * @project whale-cloud-platformX
 */
@ApiModel(value = "组件删除表接口封装实体")
public class DbDropTableEntity {

    /**
     * 组件信息
     */
    @ApiModelProperty(value = "云组件信息")
    private DbHandleEntity dbHandleEntity;

    @ApiModelProperty(value = "删表参数")
    private DropTableParamCondition dropTableParamCondition;

    public DbHandleEntity getDbHandleEntity() {
        return dbHandleEntity;
    }

    public void setDbHandleEntity(DbHandleEntity dbHandleEntity) {
        this.dbHandleEntity = dbHandleEntity;
    }

    public DropTableParamCondition getDropTableParamCondition() {
        return dropTableParamCondition;
    }

    public void setDropTableParamCondition(DropTableParamCondition dropTableParamCondition) {
        this.dropTableParamCondition = dropTableParamCondition;
    }
}
