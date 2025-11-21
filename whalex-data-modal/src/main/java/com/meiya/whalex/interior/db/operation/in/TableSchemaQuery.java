package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;

/**
 * @author zlx
 */
@ApiModel(value = "数据库表字段请求报文")
public class TableSchemaQuery extends BaseQuery {

    @ApiModelProperty(value = "数据库标识符", notes = "bigdataResourceId 与 connSetting 必须传递其中一个")
    private String bigdataResourceId;

    @ApiModelProperty(value = "云组件表信息编码")
    private String dbId;

    @NotBlank(message = "组件类型不能为空")
    @ApiModelProperty(value = "组件类型", notes = "设置 connSetting 必须传递该参数")
    private String resourceName;

    @ApiModelProperty(value = "数据库配置信息", notes = "bigdataResourceId 与 connSetting 必须传递其中一个")
    private String connSetting;

    @ApiModelProperty(value = "云组件表名")
    private String tableName;

    @ApiModelProperty(value = "云组件表信息")
    private String tableJson;

    @ApiModelProperty(value = "数据库连接类型")
    private String connType;

    @ApiModelProperty(value = "组件版本")
    private String version;

    @ApiModelProperty(value = "云厂商")
    private String cloudCode;

    @ApiModelProperty(value = "标签")
    private String tag;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getCloudCode() {
        return cloudCode;
    }

    public void setCloudCode(String cloudCode) {
        this.cloudCode = cloudCode;
    }

    public String getBigdataResourceId() {
        return bigdataResourceId;
    }

    public void setBigdataResourceId(String bigdataResourceId) {
        this.bigdataResourceId = bigdataResourceId;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    public String getConnSetting() {
        return connSetting;
    }

    public void setConnSetting(String connSetting) {
        this.connSetting = connSetting;
    }

    public String getConnType() {
        return connType;
    }

    public void setConnType(String connType) {
        this.connType = connType;
    }

    public String getResourceName() {
        return resourceName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDbId() {
        return dbId;
    }

    public void setDbId(String dbId) {
        this.dbId = dbId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableJson() {
        return tableJson;
    }

    public void setTableJson(String tableJson) {
        this.tableJson = tableJson;
    }
}
