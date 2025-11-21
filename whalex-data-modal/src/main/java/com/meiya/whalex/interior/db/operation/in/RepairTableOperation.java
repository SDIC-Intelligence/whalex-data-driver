package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;

/**
 * 修复表请求报文
 *
 * @author 黄河森
 * @date 2019/12/3
 * @project whale-cloud-platformX
 */
@ApiModel("数据库表修复请求报文")
public class RepairTableOperation extends BaseQuery {

    @ApiModelProperty(value = "组件类型")
    private String dbType;

    @ApiModelProperty(value = "数据库表标识符")
    @NotBlank(message = "dbId 不能为空")
    private String dbId;

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
}
