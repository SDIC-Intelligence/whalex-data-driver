package com.meiya.whalex.db.template.ani;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * MySql 数据库表模板
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MySqlTableConfTemplate {

    private String tableName;

    private String engine;

    public MySqlTableConfTemplate(String tableName) {
        this.tableName = tableName;
    }
}
