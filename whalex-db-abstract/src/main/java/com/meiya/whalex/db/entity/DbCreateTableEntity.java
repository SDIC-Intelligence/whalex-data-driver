package com.meiya.whalex.db.entity;

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
@ApiModel(value = "组件创建表接口封装实体")
public class DbCreateTableEntity extends DbBaseEntity implements Cloneable {

    /**
     * 查询实体
     */
    @ApiModelProperty(value = "组件表创建参数")
    private CreateTableParamCondition createTableParamCondition;

    public CreateTableParamCondition getCreateTableParamCondition() {
        return createTableParamCondition;
    }

    public void setCreateTableParamCondition(CreateTableParamCondition createTableParamCondition) {
        this.createTableParamCondition = createTableParamCondition;
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
        return JsonUtil.jsonStrToObject(objectToStr, DbCreateTableEntity.class);
    }

    public void setDbHandleEntityList(List<DbHandleEntity> dbInfoEntityList) {
        setDbInfoEntityList(dbInfoEntityList);
    }

    public List<DbHandleEntity> getDbHandleEntityList() {
        return getDbInfoEntityList();
    }
}
