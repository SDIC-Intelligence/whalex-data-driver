package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

/**
 * API 接口底层服务表创建统一入口
 *
 * @author 黄河森
 * @date 2019/9/11
 * @project whale-cloud-platformX
 */
@ApiModel(value = "组件修改表接口封装实体")
public class DbAlterTableEntity implements Cloneable {

    /**
     * 组件信息
     */
    @ApiModelProperty(value = "云组件信息")
    private DbHandleEntity dbHandleEntity;

    /**
     * 查询实体
     */
    @ApiModelProperty(value = "组件表创建参数")
    private AlterTableParamCondition alterTableParamCondition;

    public AlterTableParamCondition getAlterTableParamCondition() {
        return alterTableParamCondition;
    }

    public void setAlterTableParamCondition(AlterTableParamCondition alterTableParamCondition) {
        this.alterTableParamCondition = alterTableParamCondition;
    }

    public DbHandleEntity getDbHandleEntity() {
        return dbHandleEntity;
    }

    public void setDbHandleEntity(DbHandleEntity dbHandleEntity) {
        this.dbHandleEntity = dbHandleEntity;
    }
    /**
     * 重写Object.clone 实现深度克隆
     *
     * @return
     * @throws CloneNotSupportedException
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        String objectToStr = JsonUtil.objectToStr(this);
        return JsonUtil.jsonStrToObject(objectToStr, DbAlterTableEntity.class);
    }
}
