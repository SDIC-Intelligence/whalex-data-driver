package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.operation.in.CreateDatabaseParamCondition;
import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;


@ApiModel(value = "创建数据库接口封装实体")
public class DbCreateDatabaseEntity extends DbBaseEntity implements Cloneable  {

    /**
     * 新增实体
     */
    @ApiModelProperty(value = "创建数据库参数")
    private CreateDatabaseParamCondition createDatabaseParamCondition;

    public CreateDatabaseParamCondition getCreateDatabaseParamCondition() {
        return createDatabaseParamCondition;
    }

    public void setCreateDatabaseParamCondition(CreateDatabaseParamCondition createDatabaseParamCondition) {
        this.createDatabaseParamCondition = createDatabaseParamCondition;
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
        return JsonUtil.jsonStrToObject(objectToStr, DbCreateDatabaseEntity.class);
    }
}
