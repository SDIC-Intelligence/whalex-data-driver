package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.operation.in.MergeDataParamCondition;
import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;


/**
 * API 接口底层服务数据插入统一入口
 *
 * @author 黄河森
 * @date 2019/9/11
 * @project whale-cloud-platformX
 */
@ApiModel(value = "组件合并接口封装实体")
public class DbMergeEntity extends DbBaseEntity implements Cloneable  {

    /**
     * 新增实体
     */
    @ApiModelProperty(value = "组件合并数据参数")
    private MergeDataParamCondition mergeDataParamCondition;


    public MergeDataParamCondition getMergeDataParamCondition() {
        return mergeDataParamCondition;
    }

    public void setMergeDataParamCondition(MergeDataParamCondition mergeDataParamCondition) {
        this.mergeDataParamCondition = mergeDataParamCondition;
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
        return JsonUtil.jsonStrToObject(objectToStr, DbMergeEntity.class);
    }
}
