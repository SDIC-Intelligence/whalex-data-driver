package com.meiya.whalex.db.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;

/**
 * 删表参数实体
 *
 * @author Huanghesen
 * @date 2019/3/5
 */
@ApiModel(value = "组件删表参数")
public class DropTableParamCondition {

    @ApiModelProperty(value = "删表开始时间", notes = "周期性表可设置周期")
    private String startTime;
    @ApiModelProperty(value = "删表结束时间", notes = "周期性表可设置周期")
    private String endTime;

    @ApiModelProperty(value = "别名", notes = "只有es有用")
    private String alias;

    @ApiModelProperty(value = "如果存在", notes = "如果存在")
    private boolean ifExists;

    public DropTableParamCondition() {
    }

    @Builder
    public DropTableParamCondition(String startTime, String endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }
}
