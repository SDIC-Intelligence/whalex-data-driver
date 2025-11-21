package com.meiya.whalex.db.entity;

/**
 * 数据库表信息基类
 *
 * @author 黄河森
 * @date 2019/11/10
 * @project whale-cloud-platformX
 */
public abstract class AbstractDbTableInfo {

    private String tableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
