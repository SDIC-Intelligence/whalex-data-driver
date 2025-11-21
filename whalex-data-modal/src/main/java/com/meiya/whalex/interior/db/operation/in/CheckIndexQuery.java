package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;

/**
 * 校验资源索引，与数据库索引匹配情况请求报文
 *
 * @author 黄河森
 * @date 2019/11/28
 * @project whale-cloud-platformX
 */
@ApiModel(value = "校验资源索引与数据库索引匹配情况请求报文")
public class CheckIndexQuery extends BaseQuery {

    @ApiModelProperty(value = "资源编码")
    private String resourceId;

    @ApiModelProperty(value = "存储来源编号")
    private String dbType;

    @ApiModelProperty(value = "数据库表编码")
    private String dbId;

    @ApiModelProperty(value = "偏移量，默认0")
    private Integer offset;

    @ApiModelProperty(value = "数量，默认10")
    private Integer limit;

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

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

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }
}
