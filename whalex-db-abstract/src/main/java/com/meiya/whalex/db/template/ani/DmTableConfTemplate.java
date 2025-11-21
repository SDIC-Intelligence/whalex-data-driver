package com.meiya.whalex.db.template.ani;

import lombok.Builder;

/**
 * MySql 数据库表模板
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@Builder
public class DmTableConfTemplate {

    private String tableName;

    private Boolean ignoreCase;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Boolean getIgnoreCase() {
        return ignoreCase;
    }

    public void setIgnoreCase(Boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    public DmTableConfTemplate() {
    }

    public DmTableConfTemplate(String tableName) {
        this.tableName = tableName;
    }

    public DmTableConfTemplate(String tableName, Boolean ignoreCase) {
        this.tableName = tableName;
        this.ignoreCase = ignoreCase;
    }
}
