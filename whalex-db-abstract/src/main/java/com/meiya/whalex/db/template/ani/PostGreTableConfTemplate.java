package com.meiya.whalex.db.template.ani;

import lombok.Builder;

/**
 * PostGre 数据库表模板
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@Builder
public class PostGreTableConfTemplate {

    private String tableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public PostGreTableConfTemplate() {
    }

    public PostGreTableConfTemplate(String tableName) {
        this.tableName = tableName;
    }
}
