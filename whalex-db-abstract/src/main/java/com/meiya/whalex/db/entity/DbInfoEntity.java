package com.meiya.whalex.db.entity;

import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;

/**
 * 组件信息实体
 *
 * @author Huanghesen
 * @date 2018/9/10
 * @project whale-cloud-api
 */
public class DbInfoEntity {

    @ApiModelProperty(value = "云组件库标识符")
    @NotBlank(message = "云组件库标识符不能为空")
    private String bigResourceId;

    @ApiModelProperty(value = "云组件表标识符")
    @NotBlank(message = "云组件表标识符不能为空")
    private String dbId;

    @ApiModelProperty(value = "云组件类型")
    private String dbType;

    @ApiModelProperty(value = "云厂商代码")
    private String cloudCode;

    public DbInfoEntity() {
    }

    public DbInfoEntity(String dbId, String dbType) {
        this.dbId = dbId;
        this.dbType = dbType;
    }

    public DbInfoEntity(String bigResourceId, String dbId, String dbType) {
        this.bigResourceId = bigResourceId;
        this.dbId = dbId;
        this.dbType = dbType;
    }

    public String getDbId() {
        return dbId;
    }

    public void setDbId(String dbId) {
        this.dbId = dbId;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getBigResourceId() {
        return bigResourceId;
    }

    public void setBigResourceId(String bigResourceId) {
        this.bigResourceId = bigResourceId;
    }

    public String getCloudCode() {
        return cloudCode;
    }

    public void setCloudCode(String cloudCode) {
        this.cloudCode = cloudCode;
    }
}
