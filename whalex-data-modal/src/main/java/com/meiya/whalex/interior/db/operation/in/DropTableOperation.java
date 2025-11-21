package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;

/**
 * 删除表请求报文
 *
 * @author 黄河森
 * @date 2019/12/3
 * @project whale-cloud-platformX
 */
@ApiModel("数据库表删除请求报文")
public class DropTableOperation extends BaseQuery {

    @ApiModelProperty(value = "组件类型")
    @NotBlank(message = "必须指定组件类型")
    private String dbType;

    @ApiModelProperty(value = "数据库表标识符")
    @NotBlank(message = "必须指定数据库表标识符")
    private String dbId;

    @ApiModelProperty(value = "删表开始时间", notes = "周期性表可设置周期", access = "yyyy-MM-dd")
    private String startTime;

    @ApiModelProperty(value = "删表结束时间", notes = "周期性表可设置周期", access = "yyyy-MM-dd")
    private String endTime;

    @ApiModelProperty(value = "别名", notes = "只有es有用")
    private String alias;

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDbId() {
        return dbId;
    }

    public void setDbId(String dbId) {
        this.dbId = dbId;
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

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public DropTableOperation() {
    }

    public DropTableOperation(String role, String token, @NotBlank(message = "必须指定组件类型") String dbType, @NotBlank(message = "必须指定数据库表标识符") String dbId, String startTime, String endTime) {
        super(role, token);
        this.dbType = dbType;
        this.dbId = dbId;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public DropTableOperation(@NotBlank(message = "必须指定组件类型") String dbType, @NotBlank(message = "必须指定数据库表标识符") String dbId, String startTime, String endTime) {
        this.dbType = dbType;
        this.dbId = dbId;
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
