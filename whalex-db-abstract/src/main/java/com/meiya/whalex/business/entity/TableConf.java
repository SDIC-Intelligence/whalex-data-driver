package com.meiya.whalex.business.entity;

import java.util.Date;
import java.util.Objects;

/**
 * 表信息实体
 *
 * @author 黄河森
 * @date 2019/9/18
 * @project whale-cloud-platformX
 */
public class TableConf {

    private String id;

    private String tableName;

    private String bigdataResourceId;

    private Date updateTime;

    private String tableJson;

    private Integer isDel;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getBigdataResourceId() {
        return bigdataResourceId;
    }

    public void setBigdataResourceId(String bigdataResourceId) {
        this.bigdataResourceId = bigdataResourceId;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getTableJson() {
        return tableJson;
    }

    public void setTableJson(String tableJson) {
        this.tableJson = tableJson;
    }

    public TableConf() {
    }

    public Integer getIsDel() {
        return isDel;
    }

    public void setIsDel(Integer isDel) {
        this.isDel = isDel;
    }

    public TableConf(String id, String tableName, String bigdataResourceId, Date updateTime, String tableJson, Integer isDel) {
        this.id = id;
        this.tableName = tableName;
        this.bigdataResourceId = bigdataResourceId;
        this.updateTime = updateTime;
        this.tableJson = tableJson;
        this.isDel = isDel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableConf tableConf = (TableConf) o;
        return Objects.equals(tableName, tableConf.tableName) &&
                Objects.equals(tableJson, tableConf.tableJson);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, tableJson);
    }
}
