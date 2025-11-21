package com.meiya.whalex.db.entity;

import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

/**
 * API 接口底层服务数据删除统一入口
 *
 * @author 黄河森
 * @date 2019/9/11
 * @project whale-cloud-platformX
 */
@ApiModel(value = "组件删除接口封装实体")
public class DbDelEntity extends DbBaseEntity implements Cloneable, Serializable {

    /**
     * 查询实体
     */
    @ApiModelProperty(value = "组件删除数据参数")
    private DelParamCondition delParamCondition;


    public DelParamCondition getDelParamCondition() {
        return delParamCondition;
    }

    public void setDelParamCondition(DelParamCondition delParamCondition) {
        this.delParamCondition = delParamCondition;
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
        return JsonUtil.jsonStrToObject(objectToStr, DbDelEntity.class);
    }
}
