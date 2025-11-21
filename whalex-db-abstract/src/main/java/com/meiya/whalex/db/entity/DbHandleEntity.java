package com.meiya.whalex.db.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * 组件操作参数实体
 *
 * 当前类已经废弃，请使用
 * {@link com.meiya.whalex.db.entity.DatabaseSetting}
 * {@link com.meiya.whalex.db.entity.TableSetting}
 *
 * @author 黄河森
 * @date 2019/9/24
 * @project whale-cloud-platformX
 */
@ApiModel(value = "组件信息参数")
@Deprecated
public class DbHandleEntity implements Serializable {

    @ApiModelProperty(value = "云组件库信息编码")
    private String bigDataResourceId;
    @ApiModelProperty(value = "云组件表信息编码")
    private String dbId;
    @ApiModelProperty(value = "云组件组件类型")
    private String dbType;
    @ApiModelProperty(value = "云组件连接类型")
    private String connTypeId;
    @ApiModelProperty(value = "云组件库信息")
    private String connSetting;
    @ApiModelProperty(value = "云组件表名")
    private String tableName;
    @ApiModelProperty(value = "云组件表信息")
    private String tableJson;
    @ApiModelProperty(value = "云厂商代码")
    private String cloudCode;
    @ApiModelProperty(value = "云组件版本")
    private String version;
    @ApiModelProperty(value = "组件标签")
    private String tag;

    public String getBigDataResourceId() {
        return bigDataResourceId;
    }

    public void setBigDataResourceId(String bigDataResourceId) {
        this.bigDataResourceId = bigDataResourceId;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getConnTypeId() {
        return connTypeId;
    }

    public void setConnTypeId(String connTypeId) {
        this.connTypeId = connTypeId;
    }

    public String getConnSetting() {
        return connSetting;
    }

    public void setConnSetting(String connSetting) {
        this.connSetting = connSetting;
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

    public String getCloudCode() {
        return cloudCode;
    }

    public void setCloudCode(String cloudCode) {
        this.cloudCode = cloudCode;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public DbHandleEntity() {
    }

    public DbHandleEntity(String bigDataResourceId, String dbId, String dbType) {
        this.bigDataResourceId = bigDataResourceId;
        this.dbId = dbId;
        this.dbType = dbType;
    }

    public DbHandleEntity(String bigDataResourceId, String dbType, String connTypeId, String connSetting) {
        this.bigDataResourceId = bigDataResourceId;
        this.dbType = dbType;
        this.connTypeId = connTypeId;
        this.connSetting = connSetting;
    }

    public DbHandleEntity(String dbType, String connTypeId, String connSetting, String tableName, String tableJson) {
        this.dbType = dbType;
        this.connTypeId = connTypeId;
        this.connSetting = connSetting;
        this.tableName = tableName;
        this.tableJson = tableJson;
    }

    @Override
    public String toString() {
        return "DbHandleEntity{" +
                "bigDataResourceId='" + bigDataResourceId + '\'' +
                ", dbId='" + dbId + '\'' +
                ", resourceName='" + dbType + '\'' +
                ", connTypeId='" + connTypeId + '\'' +
                ", connSetting='" + connSetting + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tableJson='" + tableJson + '\'' +
                '}';
    }

    /**
     * 获取当前的 表标识符 或者 表名
     * @return
     */
    public String getDbIdOrTableName() {
        return StringUtils.isBlank(this.getDbId()) ? this.getTableName() : this.getDbId();
    }

    public DatabaseSetting getDatabaseSetting() {
        return DatabaseSetting.builder()
                .connSetting(this.connSetting)
                .bigDataResourceId(this.bigDataResourceId)
                .cloudCode(this.cloudCode)
                .connTypeId(this.connTypeId)
                .dbType(this.dbType)
                .version(this.version)
                .tag(this.tag)
                .build();
    }

    public TableSetting getTableSetting() {
        return TableSetting.builder()
                .tableJson(this.tableJson)
                .tableName(this.tableName)
                .dbId(this.dbId)
                .build();
    }
}
