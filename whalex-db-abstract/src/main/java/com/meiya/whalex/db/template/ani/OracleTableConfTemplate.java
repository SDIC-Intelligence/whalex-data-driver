package com.meiya.whalex.db.template.ani;

import lombok.Builder;

/**
 * Oracle 数据库表模板
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
@Builder
public class OracleTableConfTemplate {

    private String tableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public OracleTableConfTemplate() {
    }

    public OracleTableConfTemplate(String tableName) {
        this.tableName = tableName;
    }
}
