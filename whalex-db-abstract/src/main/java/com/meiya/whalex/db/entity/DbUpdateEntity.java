package com.meiya.whalex.db.entity;

import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;
import java.util.List;

/**
 * API 接口底层服务数据更新统一入口
 *
 * @author 黄河森
 * @date 2019/9/11
 * @project whale-cloud-platformX
 */
@ApiModel(value = "组件更新接口封装实体")
public class DbUpdateEntity extends DbBaseEntity implements Cloneable {

    @ApiModelProperty(value = "组件更新数据参数")
    private UpdateParamCondition updateParamCondition;

    public UpdateParamCondition getUpdateParamCondition() {
        return updateParamCondition;
    }

    public void setUpdateParamCondition(UpdateParamCondition updateParamCondition) {
        this.updateParamCondition = updateParamCondition;
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
        return JsonUtil.jsonStrToObject(objectToStr, DbUpdateEntity.class);
    }
}
