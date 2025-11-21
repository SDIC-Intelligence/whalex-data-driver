package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.operation.in.DropDatabaseParamCondition;
import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;


@ApiModel(value = "删除数据库接口封装实体")
public class DbDropDatabaseEntity extends DbBaseEntity implements Cloneable  {

    /**
     * 新增实体
     */
    @ApiModelProperty(value = "删除数据库参数")
    private DropDatabaseParamCondition dropDatabaseParamCondition;

    public DropDatabaseParamCondition getDropDatabaseParamCondition() {
        return dropDatabaseParamCondition;
    }

    public void setDropDatabaseParamCondition(DropDatabaseParamCondition dropDatabaseParamCondition) {
        this.dropDatabaseParamCondition = dropDatabaseParamCondition;
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
        return JsonUtil.jsonStrToObject(objectToStr, DbDropDatabaseEntity.class);
    }
}
