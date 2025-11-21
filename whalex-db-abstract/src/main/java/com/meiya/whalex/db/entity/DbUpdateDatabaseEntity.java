package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.operation.in.UpdateDatabaseParamCondition;
import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;


@ApiModel(value = "更新数据库接口封装实体")
public class DbUpdateDatabaseEntity extends DbBaseEntity implements Cloneable  {

    @ApiModelProperty(value = "更新数据库参数")
    private UpdateDatabaseParamCondition updateDatabaseParamCondition;

    public UpdateDatabaseParamCondition getUpdateDatabaseParamCondition() {
        return updateDatabaseParamCondition;
    }

    public void setUpdateDatabaseParamCondition(UpdateDatabaseParamCondition updateDatabaseParamCondition) {
        this.updateDatabaseParamCondition = updateDatabaseParamCondition;
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
        return JsonUtil.jsonStrToObject(objectToStr, DbUpdateDatabaseEntity.class);
    }
}
