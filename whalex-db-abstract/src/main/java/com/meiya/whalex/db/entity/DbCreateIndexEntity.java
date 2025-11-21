package com.meiya.whalex.db.entity;

import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

/**
 * API 接口底层服务索引创建统一入口
 *
 * @author 黄河森
 * @date 2019/9/11
 * @project whale-cloud-platformX
 */
@ApiModel(value = "组件索引创建接口封装实体")
public class DbCreateIndexEntity extends DbBaseEntity implements Cloneable {

    /**
     * 查询实体
     */
    @ApiModelProperty(value = "组件索引创建参数")
    private IndexParamCondition indexParamCondition;

    public IndexParamCondition getIndexParamCondition() {
        return indexParamCondition;
    }

    public void setIndexParamCondition(IndexParamCondition indexParamCondition) {
        this.indexParamCondition = indexParamCondition;
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
        return JsonUtil.jsonStrToObject(objectToStr, DbCreateIndexEntity.class);
    }
}
